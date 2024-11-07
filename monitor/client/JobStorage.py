import asyncio
from time import sleep
from typing import List

import requests
from aiohttp import ClientSession
from tqdm import tqdm

from stasis_client.client import StasisClient
from stasis_client.merge import Merge


class JobStorage:
    """
    dedicated class to store a job and provides async and sync methods
    """

    def __init__(self):
        self.paralell = True

    def store(self, job: dict, enable_progress_bar: bool, client: StasisClient):
        """
            stores the job in the db, without any form of parallelization
        """
        meta = job.pop('meta', {})
        classes = job.pop('classes', [])
        samples = job.pop('samples')
        update_acquisition = job.pop('update_acquisition', {})

        job['state'] = 'register'
        client.drop_job_samples(job['id'], enable_progress_bar=enable_progress_bar)

        response = client.http.post(f"{client._url}/job/store", json=job, headers=client._header)

        if response.status_code != 200:
            raise Exception(
                f"we observed an error. Status code was {response.status_code} and error was {response.reason} for {job}")

        sample_meta = client.compute_sample_classes(classes, update_acquisition)

        if self.paralell:
            stored_samples = self.store_samples_par(client, enable_progress_bar, job, meta, sample_meta, samples)
        else:
            stored_samples = self.store_samples(client, enable_progress_bar, job, meta, sample_meta, samples)

        job['state'] = 'entered'

        response = client.http.post(f"{client._url}/job/store", json=job, headers=client._header)
        return stored_samples

    def store_samples_par(self, client: StasisClient, enable_progress_bar, job: dict, metadata, sample_meta,
                          samples: List) -> int:
        """
            stores the samples in the database using async methods
        """

        samples = list(set(samples))

        async def main():
            from tqdm.asyncio import tqdm

            async with ClientSession() as session:
                for sample in tqdm(samples, desc="storing sample definitions (async)",
                                   disable=enable_progress_bar is False):
                    if sample in sample_meta:
                        meta = Merge().data_merge(metadata, sample_meta[sample])
                    else:
                        meta = metadata

                    to_post = {
                        "sample": sample,
                        "job": job['id'],
                        "meta": meta
                    }

                    url = f"{client._url}/job/sample/store"
                    await session.post(url, json=to_post, headers=client._header)

        asyncio.run(main())
        return len(samples)

    def store_samples(self, client, enable_progress_bar, job, meta, sample_meta, samples) -> int:
        """
        stores the actual samples in the database
        """
        stored_samples = 0
        for sample in tqdm(samples, desc="storing sample definitions", disable=enable_progress_bar is False):
            finished = 0

            if sample in sample_meta:
                meta = Merge().data_merge(meta, sample_meta[sample])

            while finished < 100:
                try:
                    to_post = {
                        "sample": sample,
                        "job": job['id'],
                        "meta": meta,
                    }

                    res = requests.post(f"{client._url}/job/sample/store", json=to_post, headers=client._header)

                    finished = 100
                    if res.status_code != 200:
                        raise Exception(
                            f"we observed an error. Status code was {res.status_code} and error was {res.reason} for {sample}")
                    stored_samples = stored_samples + 1
                except Exception as e:
                    finished = finished + 1
                    sleep(100)
        return stored_samples
