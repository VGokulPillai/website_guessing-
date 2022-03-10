import asyncio
import nest_asyncio

from concurrent.futures import ThreadPoolExecutor

nest_asyncio.apply()


class Parallelizer:
    def __init__(self, parallel_function, input_list, max_workers=5, additional_arguments=()):
        self.parallel_function = parallel_function
        self.input_list = input_list
        self.max_workers = max_workers
        self.results = []
        self.additional_args = additional_arguments

    def launch_parallelizer(self):
        self.loop = asyncio.get_event_loop()
        future = asyncio.ensure_future(self.parallelizer())
        self.loop.run_until_complete(future)

    async def parallelizer(self):
        with ThreadPoolExecutor(max_workers=self.max_workers) as executor:
            loop = asyncio.get_event_loop()
            if len(self.additional_args) != 0:
                tasks = [loop.run_in_executor(executor, self.parallel_function, inp, self.additional_args[0]) for inp in
                         self.input_list]
            else:
                tasks = [loop.run_in_executor(executor, self.parallel_function, inp) for inp in self.input_list]
            for response in await asyncio.gather(*tasks):
                self.results.append(response)
