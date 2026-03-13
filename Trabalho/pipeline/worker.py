async def worker(queue, retry_queue, fetch_fn, save_fn):

    while True:

        item_id = await queue.get()

        try:

            data = await fetch_fn(item_id)

            save_fn(data)

        except Exception as e:

            print("Erro:", item_id, e)

            await retry_queue.put(item_id)

        finally:

            queue.task_done()