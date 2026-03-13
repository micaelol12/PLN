import time

def medir_tempo(nome):

    def decorator(func):

        async def wrapper(*args, **kwargs):

            inicio = time.perf_counter()

            result = await func(*args, **kwargs)

            fim = time.perf_counter()

            print(f"{nome} levou {fim - inicio:.2f}s")

            return result

        return wrapper

    return decorator