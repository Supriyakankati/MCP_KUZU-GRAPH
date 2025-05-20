import os
import kuzu
import uvicorn
from mcp.server.fastmcp import FastMCP
from typing import Any
from starlette.applications import Starlette
from starlette.routing import Mount

# Initialize FastMCP
mcp = FastMCP(name="kuzu-knowledge-graph")

# Setup KùzuDB
KUZU_DB_PATH = "kuzu_db"
db = kuzu.Database(KUZU_DB_PATH)
conn = kuzu.Connection(db)

# Initialize schema and load data (if empty)
def init_kuzu():
    conn.execute("CREATE NODE TABLE IF NOT EXISTS User(name STRING, age INT64, PRIMARY KEY(name));")
    conn.execute("CREATE NODE TABLE IF NOT EXISTS City(name STRING, population INT64, PRIMARY KEY(name));")
    conn.execute("CREATE REL TABLE IF NOT EXISTS FOLLOWS(FROM User TO User, since INT64);")
    conn.execute("CREATE REL TABLE IF NOT EXISTS LIVES_IN(FROM User TO City);")
    
    result = conn.execute("MATCH (u:User) RETURN COUNT(u);")
    count, = result.get_next()
    
    if count == 0:
        conn.execute("COPY User FROM 'user.csv' (DELIMITER ',', HEADER FALSE);")
        conn.execute("COPY City FROM 'city.csv' (DELIMITER ',', HEADER FALSE);")
        conn.execute("COPY FOLLOWS FROM 'follows.csv' (DELIMITER ',', HEADER FALSE);")
        conn.execute("COPY LIVES_IN FROM 'lives-in.csv' (DELIMITER ',', HEADER FALSE);")

# Register MCP tools
@mcp.tool()
async def get_user_friends(name: str) -> Any:
    query = (
        "MATCH (u:User)-[f:FOLLOWS]->(f2:User) "
        "WHERE u.name = $name "
        "RETURN f2.name, f2.age, f.since;"
    )
    result = conn.execute(query, {"name": name})
    friends = []
    while result.has_next():
        friend_name, age, since = result.get_next()
        friends.append({"name": friend_name, "age": age, "since": since})
    return {"user": name, "friends": friends or "No friends found"}

@mcp.tool()
async def get_user_city(name: str) -> Any:
    query = (
        "MATCH (u:User)-[:LIVES_IN]->(c:City) "
        "WHERE u.name = $name "
        "RETURN c.name, c.population;"
    )
    result = conn.execute(query, {"name": name})
    if result.has_next():
        city_name, population = result.get_next()
        return {"user": name, "city": {"name": city_name, "population": population}}
    return {"user": name, "city": "Not found"}

@mcp.tool()
async def get_city_residents(city_name: str) -> Any:
    query = (
        "MATCH (u:User)-[:LIVES_IN]->(c:City) "
        "WHERE c.name = $city_name "
        "RETURN u.name, u.age;"
    )
    result = conn.execute(query, {"city_name": city_name})
    residents = []
    while result.has_next():
        user_name, age = result.get_next()
        residents.append({"name": user_name, "age": age})
    return {"city": city_name, "residents": residents or "No residents found"}

# Initialize DB and wrap in Starlette app
init_kuzu()

app = Starlette(
    routes=[
        Mount("/", app=mcp.sse_app()),
    ]
)

if __name__ == "__main__":
    uvicorn.run("server:app", host="0.0.0.0", port=int(os.getenv("PORT", "8002")), log_level="info")