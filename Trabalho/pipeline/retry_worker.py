async def retry_worker(retry_queue, fetch_fn, save_fn):

    while not retry_queue.empty():

        item_id = await retry_queue.get()

        try:

            data = await fetch_fn(item_id)

            save_fn(data)

        except Exception:

            print("Falha permanente:", item_id)

        finally:

            retry_queue.task_done()