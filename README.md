# Demo Data Platform API/Lib
Tools to:
- super objectively interact with the steam API to collect data about servers + players
- facilitate collecting and serving demo data
- API to wrap everything together


Installation and Development:

This project uses [PDM](https://pdm-project.org/latest/) for development.

Note that it can still be installed with `pip` into an existing project from source.


clone and install:

```sh
git clone https://github.com/MegaAntiCheat/api.git
cd api
pdm sync

# `pdm sync -G:all` for development dependencies
```
# Usage

## Authenticating with the API:
Visit https://megaanticheat.com/provision to provision an API key, and store the API key in your `config.yaml` file. See https://github.com/MegaAntiCheat/client-backend
for instructions on where to locate it, but basically in a format of
```yaml
masterbase_key: 'your_api_key_here'
```


# Development


## Steam API

First one needs to make their steam API available. You can just use it in function calls like below:

```py
from api.servers import Query


filters = {
    "appid": 440,
    "empty": False,
    "linux": True,
    "gametype": "valve"
}

limit = 1

servers = Query("MY_STEAM_API_KEY", filters, limit).query()

for server in servers:
    print(server)

    server_info = server.query("MY_STEAM_API_KEY")
    print(server_info)
```
Results in:
```json
addr='169.254.189.228:41928' gameport=41928 steamid='90178913460028417' name='Valve Matchmaking Server (Washington srcds1002-eat1 #94)' appid=440 gamedir='tf' version='8604597' product='tf' region=255 players=24 max_players=32 bots=0 map='pl_pier' secure=True dedicated=True os='l' gametype='hidden,increased_maxplayers,payload,valve' URL='https://api.steampowered.com/IGameServersService/QueryByFakeIP/v1/' QUERY_TYPES={1: 'ping_data', 2: 'players_data', 3: 'rules_data'}
{
    "ping_data": {
        "server_ip": {
            "v4": 2852044260
        },
        "query_port": 41928,
        "game_port": 41928,
        "server_name": "Valve Matchmaking Server (Washington srcds1002-eat1 #94)",
        "steamid": "90178913460028417",
        "app_id": 440,
        "gamedir": "tf",
        "map": "pl_pier",
        "game_description": "Team Fortress",
        "gametype": "hidden,increased_maxplayers,payload,valve",
        "num_players": 23,
        "max_players": 32,
        "num_bots": 0,
        "password": false,
        "secure": true,
        "dedicated": true,
        "version": "8604597",
        "sdr_popid": 7173992
    },
    "players_data": {
        "players": [
            {
                "name": "Javaris Jamar Javarison-Lamar",
                "score": 0,
                "time_played": 3079
            },
            {
                "name": "DamitriusDamarcusBartholamyJame",
                "score": 1,
                "time_played": 2733
            },
            {
                "name": "joe",
                "score": 1,
                "time_played": 1800
            },
            {
                "name": "soysauce20001",
                "score": 2,
                "time_played": 1302
            },
            {
                "name": "Buhda",
                "score": 4,
                "time_played": 1153
            }
        ]
    }
}
```

One can also make their steam api key available through the helper methods in `src/api/auth.py` through a json, toml, or environment variable.


# Development:

## Launching

Highly recommend that devs have some sort of postgres tool installed to connect/view data. I prefer [pgcli](https://www.pgcli.com/).

Needed:
- python `3.10` or greater
- docker

A few environment variables are needed in order to correctly run this.
`POSTGRES_USER` and `POSTGRES_PASSWORD`.  These are used in containers and are what the DB and API will be configured with.

We also will need a docker network to link the two containers.

```sh
docker network create --driver bridge masterbase-network
```

Build the DB:

```sh
docker build -f Dockerfile.db . -t db
```

Run the DB:

```sh
docker run --network=masterbase-network -p 8050:5432 -e POSTGRES_USER=$POSTGRES_USER -e POSTGRES_PASSWORD=$POSTGRES_PASSWORD -e POSTGRES_DB=demos -t db
```

Run migrations:

```sh
pdm run alembic upgrade head
```

Build the API:

```sh
docker build -f Dockerfile.api . -t api
```

Now before we run, we need to inject the IPv4 of the db container. We can find this by running `docker network inspect masterbase-network` and inspecting the output. In my case, I see the IP as `172.20.0.2`

Run the API:

```sh
docker run -p 8000:8000 --network=masterbase-network -e POSTGRES_USER=$POSTGRES_USER -e POSTGRES_PASSWORD=$POSTGRES_PASSWORD -e POSTGRES_HOST=172.20.0.2 -e POSTGRES_PORT=5432 -t api
```

Endpoints are located behind `http://localhost:8000/`.

I also recommend `Dev Containers` vscode plugin that makes attaching and monitoring containers very easy.

Note that the DB schema is updated/set on deploy for convenience.

To inspect the DB from your computer (not inside a container), the connection string will be `postgresql://${POSTGRES_USER}:${POSTGRES_PASSWORD}@localhost:8050/demos`


## Database API:

This is a Litestar API and can be invoked by running (calls uvicorn under the hood) the following:

`pdm run app`

Note that this is really for development purposes and a convenience way to start the application.

Documentation is autogenerated at the `/schema` endpoint, by Litestar :D

General workflow is as follows:
- hit the `/provision` endpoint to authenticate through steam, which will in return give the user an API key back as a manual step.
- `/session_id` is used to start a session from the client or dev and wil return a session ID key, that is used in further request to stream data and close the session. A user can only be in one session at a time, and only if they have an API key, so do not lose either.
- `/demos` is a websocket connection endpoint, and is used to stream data and only accepts bytes once established. This endpoint will only work if the user is in a session and requires an API key and a session ID key on connection: `ws://127.0.0.1:8000/demos?api_key=your_api_key&session_id=your_session_id`.

The following is how one would interact with the API in python:

Once the app is launched, head to `http://127.0.0.1:8000/provision` and sign in. The webpage will display your API key and can be fed into subsequent requests.


```py
"""Example on how to get a session ID."""
import requests

api_key = YOUR_API_KEY_HERE

app_host = "127.0.0.1:8000"

response = requests.get(f"http://{app_host}/session_id", params={
    "api_key": api_key,
    "fake_ip": "123.45.6789",  # IP of current server/game
    "map": "asdf"  # map of current server/game
})

print(response.json())  # Session ID that was persisted to database
```

Once you have a session id, you can create a websocket connection and start streaming data if it is accepted.

```py
import websockets
import asyncio


api_key = YOUR_API_KEY_HERE
session_id = YOUR_SESSION_ID_HERE

app_host = "127.0.0.1:8000"


async def send_demo_file():
    uri = f"ws://{app_host}/demos?api_key={api_key}&session_id={session_id}"
    async with websockets.connect(uri) as socket:
        # for this example we are using a static demo file but can stream any bytes
        with open("some_demo_file.dem", "rb") as f:
            while True:
                chunk = f.read(1024)
                if not chunk:
                    # session is auto closed on disconnect.
                    break
                await socket.send(chunk)


async def test_demo_streaming() -> None:
    """Test that a demo is completely received by the API and sunk to a file."""
    await send_demo_file(session_id, api_key)

async def main():

    await test_demo_streaming()

# Run the event loop
if __name__ == "__main__":
    asyncio.run(main())
```

## Database:

This is a postgres database with migrations/schemas managed by Alembic

Schemas are located in `alembic/versions`

Creating a revision is as simple as:

`pdm run alembic revision -m revision-name`

Upgrading and downgrading:

(note that this needs to be done from inside the container currently and happens on deploy)

`pdm run alembic upgrade head`

`pdm run alembic downgrade -1`

It is important that downgrades completely undo an upgrade, as to not get stuck.
