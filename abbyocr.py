"""
Send files to Abbyy OCR and save the results
"""
import os
import json
import asyncio
import logging
import xml.dom.minidom

import aiofiles
import aiohttp

from slugify import UniqueSlugify

logging.basicConfig(
    format='%(asctime)s - %(levelname)s -  %(funcName)15s() -  %(message)s',
    level=logging.INFO
)


class ProcessingSettings:  # pylint: disable=too-few-public-methods
    """
    Base settings
    """
    language = "English"
    format = "docx"


class Task:  # pylint: disable=too-few-public-methods
    """
    Task data structure
    """

    def __init__(self):
        self.status = "Unknown"
        self.id = None  # pylint: disable=invalid-name
        self.download_url = None
        self.file_name = None
        self.is_active = self._is_active()

    def _is_active(self):
        return self.status in ("InProgress", "Queued")

    def ready(self):
        """
        Check task is ready or not
        :return:
        """
        return self.status == 'Completed'

    def failed(self):
        """
        Is task failed
        :return:
        """
        return self.status == 'ProcessingFailed'

    def json_dump():
        return json.dumps(self.__dict__)

    def __str__(self):
        return self.json_dump()

    def __repr__(self):
        return self.json_dump()

    def dict(self):
        """
        A wraper for the magic method
        :return:
        """
        return self.__dict__


class AbbyOCR:
    """
    Class for the Abbyy OCR
    """

    base_url = 'https://cloud.ocrsdk.com/'
    logger = logging.getLogger(__name__)
    slugify = UniqueSlugify()
    _destination_folder = None
    timeout = 60 * 10

    def __init__(  # pylint: disable=too-many-arguments
            self, event_loop, destination_folder, input_file, input_folder,
            abbyy_login=None, abbyy_password=None
    ):
        self.destination_folder = destination_folder
        self.input_file = input_file
        self.input_folder = input_folder
        self.event_loop = event_loop

        self.session = aiohttp.ClientSession(
            auth=aiohttp.helpers.BasicAuth(abbyy_login, abbyy_password),
            loop=event_loop
        )

        self.download_session = aiohttp.ClientSession(
            loop=event_loop
        )

    async def clean(self):
        """
        Close open sessions in the end of execution
        :return:
        """
        self.logger.info('close sessions')
        await self.session.close()
        await self.download_session.close()

    @classmethod
    def run(  # pylint: disable=too-many-arguments
            cls, destination_folder, input_file, input_folder,
            abbyy_login=None, abbyy_password=None
    ):
        """
        Open event loop and run class entry point
        :param destination_folder: Path to the results folder
        :param input_file: path to the file with the files list
        :param input_folder: path to the folder with the files for treatment
        :param abbyy_login:  abbyy login
        :param abbyy_password: abby password
        :return:
        """
        cls.logger.info('AbbyOCR started')
        event_loop = asyncio.get_event_loop()
        try:
            event_loop.run_until_complete(
                cls.main(
                    event_loop, destination_folder, input_file,
                    input_folder, abbyy_login, abbyy_password
                )
            )
        finally:
            event_loop.close()
        cls.logger.info('AbbyOCR executed')

    @classmethod
    async def main(  # pylint: disable=too-many-arguments
            cls, event_loop, destination_folder, input_file,
            input_folder, abbyy_login, abbyy_password
    ):
        """
        Class entry point. Create and run producer and consumers
        :param event_loop:
        :param destination_folder:
        :param input_file:
        :param input_folder:
        :param login:
        :param password:
        :return:
        """
        num_consumers = 5
        instance = cls(
            event_loop, destination_folder, input_file,
            input_folder, abbyy_login, abbyy_password
        )
        # Create the queue with a fixed size so the producer
        # will block until the consumers pull some items out.
        cls.logger.info('Files queues created')
        files_queue = asyncio.Queue(maxsize=num_consumers * 2)
        # Scheduled the consumer tasks.
        consumers = [
            event_loop.create_task(instance.download_files(files_queue, n))
            for n in range(num_consumers)
        ]
        cls.logger.info('Created %s consumers', num_consumers)
        # Schedule the producer task.
        prod = event_loop.create_task(instance.upload_files(files_queue, num_consumers))
        cls.logger.info('Created producer')
        # Wait for all of the coroutines to finish.
        cls.logger.info('Start processing')
        await asyncio.wait(consumers + [prod])
        cls.logger.info('Close sessions')
        await instance.clean()

    async def upload_files(self, queue, num_workers):
        """
        Producer. Upload file to the API
        :param queue: queue of the tasks to sync consumers
        :param num_workers: number of async uploads
        :return:
        """
        self.logger.info('Files uploading started')
        # Add some numbers to the queue to simulate jobs

        routines = []
        for file in open(self.input_file):
            file = file.strip()
            routines.append(self.upload_file(file, queue))
            if len(routines) >= num_workers:
                await asyncio.wait(routines)
                self.logger.info('Upload batch %s', routines)
                routines = []
        if routines:
            self.logger.info('Upload batch %s', routines)
            await asyncio.wait(routines)

        # Add None entries in the queue
        # to signal the consumers to exit
        self.logger.info('Added stop signals to the queue')
        for _ in range(num_workers):
            await queue.put(None)
        self.logger.info('Waiting for the empty queue')
        await queue.join()
        self.logger.info('Ending')

    async def download_files(self, queue, worker_number):
        """
        Consumer. Async worker to download files
        from the api
        :param queue: tasks queue to synchronize workers
        :param worker_number: number of worker for debug
        :return:
        """
        self.logger.info('consumer %s: starting', worker_number)
        while True:
            self.logger.info('consumer %s: waiting for task', worker_number)
            task = await queue.get()
            self.logger.info('consumer %s: has task %s', worker_number, task)
            if task is None:
                # None is the signal to stop.
                queue.task_done()
                break
            else:
                completed_task = await self.get_completed_task(task.id)
                if completed_task:
                    completed_task.file_name = task.file_name
                    await self.download_and_save_file(completed_task)
                    await self.delete_task(completed_task.id)
                    self.logger.info('consumer %s: task %s is done', worker_number, task)
                queue.task_done()

        self.logger.info('consumer %s: ending', worker_number)

    async def upload_file(self, path, queue):
        """
        Upload file to the api
        :param path:
        :param file_name:
        :return:
        """
        self.logger.info(path)
        try:
            data = await self.read_file(path)
        except OSError as os_exception:
            self.logger.warning('Can\'t read file %s (%s)', path, str(os_exception))
            return
        url_params = {
            "language": ProcessingSettings.language,
            "exportFormat": ProcessingSettings.format
        }
        request_url = self.base_url + "processImage"
        task = await self._make_request(
            method='POST',
            url=request_url,
            data=data,
            params=url_params,
            return_first=True
        )
        file_name = self.get_file_name(path)
        self.logger.info('Received: %s (%s)', task, file_name)
        if task:
            task.file_name = file_name
            await queue.put(task)

    async def read_file(self, path):
        """
        Read file from disk
        :param path:
        :return:
        """
        async with aiofiles.open(
            os.path.join(self.input_folder, path), 'rb', loop=self.event_loop
        ) as image_file:
            data = await image_file.read()
            return data

    async def _make_request(  # pylint: disable=too-many-arguments
            self, method, url, data=None, params=None, return_first=False
    ):
        with aiohttp.Timeout(self.timeout):
            async with self.session.request(
                method=method, url=url, data=data, params=params
            ) as resp:
                self.logger.info('method: %s, url: %s; response code: %s', method, url, resp.status)
                res_data = await resp.read()
                if resp.status == 200:
                    tasks = self.decode_response(res_data)
                    if return_first:
                        tasks = tasks.pop()
                    return tasks
                else:
                    self.logger.warning(
                        'method: %s, url: %s; response code: %s; response: %s',
                        method, url, resp.status, res_data
                    )

    async def get_completed_task(self, task_id):
        """
        Wait for the completed task and return it
        :param task_id: task id from the api
        :return:
        """
        request = {
            'url': self.base_url + 'getTaskStatus',
            'method': 'GET',
            'params': {'taskId': task_id},
            'return_first': True,
            'data': None
        }
        counter = 1
        while True:
            self.logger.info('Get status for task %s', task_id)
            sleep = counter * 2
            task = await self._make_request(**request)
            self.logger.info('Received task %s', task_id)
            if task is None:
                self.logger.warning('Task %s is None', task_id)
                return None
            elif task.ready():
                self.logger.info('Task %s is ready, url: %s', task.id, task.download_url)
                return task
            elif task.failed():
                self.logger.warning('Task %s is failed (%s)', task.id, task)
                return None
            self.logger.info(
                'Task %s is not ready, sleep %s sec (retry %s)', task.id, sleep, counter
            )
            await asyncio.sleep(sleep)
            counter += 1
            if counter > 1000000:
                break

    async def download_and_save_file(self, task):
        """
        Download file and save it on disk
        :param task:
        :return:
        """
        self.logger.info(task)
        path = os.path.join(self.destination_folder, task.file_name)
        self.logger.info('Download task %s with url %s', task.id, task.download_url)
        with aiohttp.Timeout(self.timeout):
            async with aiofiles.open(path, 'wb', loop=self.event_loop) as destination_file, \
                    self.download_session.get(task.download_url) as resp:
                self.logger.info(
                    'Task %s with url %s has code %s', task.id, task.download_url, resp.status
                )
                self.logger.info('Write task %s to the file %s', task.id, path)
                while True:
                    response_chunk = await resp.content.read(1024)
                    if not response_chunk:
                        break
                    await destination_file.write(response_chunk)

    async def delete_task(self, task_id):
        """
        Remove task from the API
        :param task_id:
        :return:
        """
        self.logger.info('Delete task %s', task_id)
        request = {
            'url': self.base_url + 'deleteTask',
            'method': 'GET',
            'params': {'taskId': task_id},
            'return_first': True,
            'data': None
        }
        task = await self._make_request(**request)
        return task

    def decode_response(self, xml_response):  # pylint: disable=no-self-use
        """
        Decode xml response of the server. Return Task object
        """
        dom = xml.dom.minidom.parseString(xml_response)
        task_nodes = dom.getElementsByTagName("task")
        res = []
        for task_node in task_nodes:
            task = Task()
            task.id = task_node.getAttribute("id")
            task.status = task_node.getAttribute("status")
            if task.status == "Completed":
                task.download_url = task_node.getAttribute("resultUrl")
            res.append(task)
        return res

    def get_file_name(self, path):
        """
        Slugify file name and add docx suffix
        :param path:
        :return:
        """
        name = '{}.docx'.format(self.slugify(path))
        self.logger.info('Generated name %s for file %s', name, path)
        return name

    @property
    def destination_folder(self):
        """
        destination folder getter
        :return:
        """
        return self._destination_folder

    @destination_folder.setter
    def destination_folder(self, folder_path):
        """
        Destination folder setter
        :param v:
        :return:
        """
        if not os.path.exists(folder_path):
            os.makedirs(folder_path)
        self._destination_folder = folder_path


if __name__ == '__main__':
    import argparse

    PARSER = argparse.ArgumentParser()

    PARSER.add_argument(
        "input_file",
        help="Full or relative path to the input file",
        type=str
    )
    PARSER.add_argument(
        "input_folder",
        help="Full or relative path to the input folder",
        type=str
    )

    PARSER.add_argument(
        "destination_folder",
        help="a full path to the destination folder",
        type=str
    )

    PARSER.add_argument(
        "login",
        help="Abby login",
        type=str
    )

    PARSER.add_argument(
        "password",
        help="Abby password",
        type=str
    )

    ARGS = PARSER.parse_args()
    DESTINATION_FOLDER_PATH = ARGS.destination_folder
    INPUT_FILE_PATH = ARGS.input_file
    INPUT_FOLDER_PATH = ARGS.input_folder
    LOGIN = ARGS.login
    PASSWORD = ARGS.password

    AbbyOCR.run(
        DESTINATION_FOLDER_PATH, INPUT_FILE_PATH, INPUT_FOLDER_PATH,
        LOGIN, PASSWORD
    )
