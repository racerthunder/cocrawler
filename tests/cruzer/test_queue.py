import asyncio


deffered_queue = asyncio.Queue()



async def worker():

    for i in range(5):
        deffered_queue.put_nowait('item_{0}'.format(i))

    while True:
        item = None
        try:
            raise ValueError('-->')
            #item = deffered_queue.get_nowait()

        except asyncio.queues.QueueEmpty:
            print('empty')
            await asyncio.sleep(1)

        except Exception as ex:
            print('--> catching all exceptions')
            await  asyncio.sleep(1)

        print('item: ',item)
        if item is not None:
            print('--> THE END')
        #break

async def other():
    print('--> otehr is called')
    await asyncio.sleep(1)

async def main():
    deffered = asyncio.Task(worker())
    others = [asyncio.Task(other()) for _ in range(2)]

    if deffered.done():
        print('--> canceling otehr')
        for task in others:
            task.cancel()



if __name__ == '__main__':
    loop = asyncio.get_event_loop()
    loop.run_until_complete(main())

