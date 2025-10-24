import asyncio

# seconds
LATENCY = 0.25
TARGET_HOST = '127.0.0.1'
TARGET_PORT = 19000
LISTEN_PORT = 20000

async def handle_connection(reader, writer):
    target_reader, target_writer = await asyncio.open_connection(TARGET_HOST, TARGET_PORT)

    async def forward(source, dest):
        try:
            while True:
                data = await source.read(1024)
                if not data:
                    break
                await asyncio.sleep(LATENCY)
                dest.write(data)
                await dest.drain()
        except asyncio.CancelledError:
            pass

    await asyncio.gather(
        forward(reader, target_writer),
        forward(target_reader, writer)
    )

async def main():
    server = await asyncio.start_server(handle_connection, '127.0.0.1', LISTEN_PORT)
    async with server:
        await server.serve_forever()

asyncio.run(main())
