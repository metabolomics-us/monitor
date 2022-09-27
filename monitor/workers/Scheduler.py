from queue import Queue
from threading import Thread
from time import sleep

from cisclient.client import CISClient
from cisclient.exceptions import CisClientException
from loguru import logger
from stasis_client.client import StasisClient

from monitor.exceptions import NoProfileException, SampleNotFoundException, JobDataStoreException


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

                logger.info(f'Scheduling sample with id {item}')

                job = self.schedule_sample(item)
                logger.info(f'\tJob id: {job}')

                if job:
                    logger.info(f'\tSchedule successful. Job id {job["job"]}')
                else:
                    self.fail_sample(item)

            except KeyboardInterrupt:
                logger.warning(f'Stopping {self.name} due to Keyboard Interrupt')
                self.running = False
                self.schedule_q.queue.clear()
                self.schedule_q.join()
                self.parent.join_threads()

            except IndexError:
                sleep(1)

            except SampleNotFoundException:
                logger.error(f'\tAcquisition data for sample {item} not found', exc_info=True)
                self.fail_sample(item)

            except CisClientException as ex:
                logger.error(f'\tCisClient error: {ex.args}', exc_info=True)
                self.fail_sample(item)

            except NoProfileException as ex:
                logger.error(f'\tScheduling error: {ex.args}', exc_info=True)
                self.fail_sample(item)

            except JobDataStoreException as ex:
                logger.error(f"Error scheduling job {ex.args}")
                self.fail_sample(item)

            except Exception as ex:
                logger.error(f'\tError scheduling sample {item}: {ex.args}', exc_info=True)
                self.fail_sample(item)

            finally:
                self.schedule_q.task_done()
                logger.info(f'Scheduler queue size: {self.schedule_q.qsize()}')

        logger.info(f'Stopping {self.name}')
        self.join()

    def schedule_sample(self, sample_id):
        sample_data = {}

        try:
            # get sample data.
            sample_data = self.stasis_cli.sample_acquisition_get(sample_id)
        except Exception as ex:
            logger.error(f"\tCan't get sample '{sample_id}' metadata. StasisClient error: {ex.args}", exc_info=True)
            if 'acquisition data not found' in str(ex):
                raise SampleNotFoundException(sample=sample_id)

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

        except CisClientException:
            raise
        except NoProfileException:
            raise
        except Exception:
            raise

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
                raise JobDataStoreException(job=job['id'])

        except JobDataStoreException:
            raise
        except Exception as ex:
            logger.error(f'Error storing or scheduling data: {ex.args}', exc_info=True)
            raise

    def _get_latest_version(self, method):
        versions = self.cis_cli.get_method_last_version(method)

        def get_version(item):
            return item['updated']

        if not versions:
            return 'fixed'
        else:
            vsorted = sorted(versions, reverse=True, key=get_version)

            return vsorted[0]['version']

    def fail_sample(self, file_basename):
        try:
            logger.error(f'\tAdd "failed" status to stasis for sample "{file_basename}"')
            self.stasis_cli.sample_state_update(file_basename, 'failed')
        except Exception as ex:
            logger.error(f'\tStasis client can\'t send "failed" status for sample {file_basename}\n'
                         f'\tResponse: {str(ex)}')

