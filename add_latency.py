import asyncio, time

# seconds (one-way latency to add)
LATENCY = 0.5
TARGET_HOST = '127.0.0.1'
TARGET_PORT = 19000
LISTEN_PORT = 20000

async def handle_connection(reader, writer):
    target_reader, target_writer = await asyncio.open_connection(TARGET_HOST, TARGET_PORT)

    async def forward(source, dest, *, direction):
        buffer = bytearray()
        deadline = None
        flush_task = None
        closed = False

        async def flusher():
            nonlocal buffer, deadline, flush_task
            # Sleep until the scheduled deadline
            now = time.perf_counter()
            to_sleep = max(0.0, (deadline or now) - now)
            t0 = time.perf_counter()
            await asyncio.sleep(to_sleep)
            slept = time.perf_counter() - t0

            # Ship everything we accumulated in this window
            data = bytes(buffer)
            buffer.clear()
            deadline = None
            flush_task = None

            dest.write(data)
            await dest.drain()
            #print(f"[{direction}] released {len(data)} bytes "
            #      f"after ~{slept:.6f}s (requested {LATENCY:.6f}s)")

        try:
            while True:
                # Large read size to avoid unnecessary fragmentation
                data = await source.read(65536)
                if not data:
                    # Source closed; if something is pending, wait for it to flush
                    if flush_task is not None:
                        await flush_task
                    break

                buffer += data
                # Schedule a single flush LATENCY in the future if not already scheduled
                if flush_task is None:
                    deadline = time.perf_counter() + LATENCY
                    flush_task = asyncio.create_task(flusher())

            # Propagate EOF
            try:
                dest.write_eof()
            except Exception:
                pass
        except asyncio.CancelledError:
            pass

    await asyncio.gather(
        forward(reader, target_writer, direction="client→server"),
        forward(target_reader, writer, direction="server→client"),
    )

async def main():
    server = await asyncio.start_server(handle_connection, '127.0.0.1', LISTEN_PORT)
    async with server:
        await server.serve_forever()

asyncio.run(main())
