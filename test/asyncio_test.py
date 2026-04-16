import asyncio  

async def main():
    print("Hello")
    yield 10
    print("World")
    await asyncio.sleep(1)
    print("World")