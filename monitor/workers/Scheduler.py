from queue import Queue
from threading import Thread
from time import sleep

from cisclient.client import CISClient
from cisclient.exceptions import CisClientException
from loguru import logger
from stasis_client.client import StasisClient

from monitor.exceptions import NoProfileException


class Scheduler(Thread):
    """
    Worker class that schedules samples for preprocessing on AWS
    """

    def __init__(self, parent, stasis: StasisClient, cis: CISClient,
                 config, sched_q: Queue,
                 name='Scheduler0', daemon=True):
        """

        Args:
            parent:
                Instance parent object
            stasis: StasisClient
                A stasis client instance
            cis: CisClient
                A cis client instance
            config:
                An object containing config settings
            sched_q: Queue
                A queue that contains the samples to be auto-scheduled
            name: str (Optional. Default: Scheduler0)
                Name of the worker instance
            daemon:
                Run the worker as daemon. (Optional. Default: True)
        """
        super().__init__(name=name, daemon=daemon)
        self.parent = parent
        self.stasis_cli = stasis
        self.cis_cli = cis
        self.schedule_q = sched_q
        self.running = False
        self.schedule = config['monitor']['schedule']
        self.test = config['test']

    def run(self):
        item = None
        self.running = True

        while self.running:
            try:
                item = self.schedule_q.get()

                # if self.test:
                #     logger.info(f'Fake Scheduling sample with id {item}')
                # else:
                logger.info(f'Scheduling sample with id {item}')

                job = self.schedule_sample(item)
                logger.info(f'job: {job}')

                if job:
                    logger.info(f'Schedule successful. Job id {job["job"]}')

            except KeyboardInterrupt:
                logger.warning(f'Stopping {self.name} due to Keyboard Interrupt')
                self.running = False
                self.schedule_q.queue.clear()
                self.schedule_q.join()
                self.parent.join_threads()

            except IndexError:
                sleep(1)

            except Exception as ex:
                logger.error(f'Error scheduling sample {item}: {ex.args}', exc_info=True)

            finally:
                self.schedule_q.task_done()
                logger.info(f'Scheduler queue size: {self.schedule_q.qsize()}')

        logger.info(f'Stopping {self.name}')
        self.join()

    def schedule_sample(self, sample_id):
        try:
            # get sample data.
            sample_data = self.stasis_cli.sample_acquisition_get(sample_id)
        except Exception as ex:
            logger.error(f"Can't get sample '{sample_id}' metadata.\nStasisClient error: {ex.args}", exc_info=True)
            return

        try:
            # build method string
            method = ' | '.join([sample_data['chromatography']['method'], sample_data['chromatography']['instrument'],
                                 sample_data['chromatography']['column'], sample_data['chromatography']['ionisation']])

            version = self._get_latest_version(method)
            logger.info(f'latest method version: ' + version)

            # get profile data
            profile_list = self.cis_cli.get_unique_method_profiles(method, version)

            if len(profile_list) <= 0:
                raise NoProfileException(f"Can't find valid profiles for method '{method}' and version '{version}'")

            profiles = ','.join([x['profile'] for x in profile_list])

        except CisClientException as ex:
            logger.error(f'CisClient error: {ex.args}', exc_info=True)
            raise ex
        except NoProfileException as ex:
            logger.error(f'Scheduling error: {ex.args}', exc_info=True)
            raise ex
        except Exception as ex:
            logger.error(f'Error gathering sample data: {ex.args}', exc_info=True)
            raise ex

        job = {
            'id': f'preprocess_{sample_id}',  # in case we need more uniqueness: {time.strftime("%Y%m%d-%H%M%S")}_
            'method': method,
            'profile': profiles,
            'samples': [sample_id]
        }

        try:
            logger.debug('storing job...')
            stored_samples = self.stasis_cli.store_job(job)

            logger.info(f"Job {job['id']} stored successfully.")

            if stored_samples:
                response = self.stasis_cli.schedule_job(job['id'])

                logger.info(f"Sample {sample_id} scheduled successfully.")
                return response
            else:
                logger.error(f"Error scheduling job {job['id']}")
                return

        except Exception as ex:
            logger.error(f'Error storing or scheduling data: {ex.args}', exc_info=True)
            raise ex

    def _get_latest_version(self, method):
        versions = self.cis_cli.get_method_last_version(method)

        def get_version(item):
            return item['updated']

        if not versions:
            return 'fixed'
        else:
            vsorted = sorted(versions, reverse=True, key=get_version)

            return vsorted[0]['version']
