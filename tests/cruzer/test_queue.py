import asyncio


main_queue = asyncio.Queue(maxsize=1)
deffered_queue = asyncio.Queue()

async def deffered_processor():
    while True:
        await main_queue.put('--deffered_item')
        print('--> deffered item added')

async def queue_producer():


    while True:
        await asyncio.sleep(1)
        try:

            ridealong = deffered_queue.get_nowait()

            print(ridealong)

        except asyncio.queues.QueueEmpty:
            print('--> main queue is empty')
            break


def main():
    producer = asyncio.Task(queue_producer())
    deffered = asyncio.Task(deffered_processor())


if __name__ == '__main__':
    loop = asyncio.get_event_loop()
    loop.run_until_complete(main())

