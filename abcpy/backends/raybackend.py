from ast import Pass
from abcpy.backends import Backend, PDS
import ray
import uuid
import numpy as np

import sys
import ray.cloudpickle as pickle
pickle.register_pickle_by_value(sys.modules[__name__])


@ray.remote
class RayActor:

    def __init__(self, index):
        self.data = {}
        self.rng = np.random.RandomState(index)

    def add_data(self, uid, data):
        self.data[uid] = data

    def map(self, f, uid, new_uid):
        new_data = []
        for i in self.data[uid]:
            new_data.append(f(i))
        self.data[new_uid] = new_data

    def map_random(self, f, uid, new_uid):
        new_data = []
        for i in self.data[uid]:
            new_data.append(f(self.rng, i))
        self.data[new_uid] = new_data

    def remove(self, uid):
        del self.data[uid]

    def compute(self, uid):
        data = self.data[uid]
        return data


class Rdd:

    def __init__(self, uid, pool):
        self.uid = uid
        self.pool = pool

    def __del__(self):
        self.pool.remove(self.uid)


class RayActorPool:

    def __init__(self, n):
        self.actors = [RayActor.remote(i) for i in range(n)]
        self.n = n

    def parallelize(self, data):
        if not isinstance(data, list):
            data = data.tolist()

        n = len(data)
        batch_size = n // self.n
        batches = [data[i: i + batch_size] for i in range(0, len(data), batch_size)]
        
        uid = uuid.uuid1()
        for i, d in zip(self.actors, batches):
            i.add_data.remote(uid, d)
        return Rdd(uid, self)

    def remove(self, uid):
        for i in self.actors:
            i.remove.remote(uid)

    def compute(self, rdd):
        res = []
        for i in self.actors:
            res += ray.get(i.compute.remote(rdd.uid))
        return res

    def map(self, f, rdd):
        new_uid = uuid.uuid1()
        for i in self.actors:
            i.map.remote(f, rdd.uid, new_uid)
        return Rdd(new_uid, self)

    def map_random(self, f, rdd):
        new_uid = uuid.uuid1()
        for i in self.actors:
            i.map_random.remote(f, rdd.uid, new_uid)
        return Rdd(new_uid, self)



class BackendRay(Backend):
    """
    A parallelization backend for Apache Spark. It is essetially a wrapper for
    the required Dask on Ray functionality.
    """

    def __init__(self, n_partitions):
        """
        Initialize the backend with an existing and configured SparkContext.

        Parameters
        ----------
        sparkContext: pyspark.SparkContext
            an existing and fully configured PySpark context
        parallelism: int
            defines on how many workers a distributed dataset can be distributed
        """
        self.pool = RayActorPool(n_partitions)

    def parallelize(self, python_list):
        """
        This is a wrapper of pyspark.SparkContext.parallelize().

        Parameters
        ----------
        list: Python list
            list that is distributed on the workers
        
        Returns
        -------
        PDSSpark class (parallel data set)
            A reference object that represents the parallelized list
        """

        return self.pool.parallelize(python_list)

    def broadcast(self, object):
        """
        This is a wrapper for pyspark.SparkContext.broadcast().

        Parameters
        ----------
        object: Python object
            An abitrary object that should be available on all workers
        Returns
        -------
        BDSSpark class (broadcast data set)
            A reference to the broadcasted object
        """
        bcv = ray.put(object)
        bds = BDSRay(bcv)
        return bds

    def map(self, func, pds):
        """
        This is a wrapper for pyspark.rdd.map()

        Parameters
        ----------
        func: Python func
            A function that can be applied to every element of the a bag
        pds: Ray Bag class
            A parallel data set to which func should be applied
        Returns
        -------
        PDSSpark class
            a new parallel data set that contains the result of the map
        """
        return self.pool.map(func, pds)


    def map_random(self, func, pds):
        """
        This is a wrapper for pyspark.rdd.map()

        Parameters
        ----------
        func: Python func
            A function that can be applied to every element of the a bag
        pds: Ray Bag class
            A parallel data set to which func should be applied
        Returns
        -------
        PDSSpark class
            a new parallel data set that contains the result of the map
        """
        return self.pool.map_random(func, pds)


    def collect(self, pds):
        """
        A wrapper for pyspark.rdd.collect()

        Parameters
        ----------
        pds: Dask Bag
        Returns
        -------
        Python list
            all elements of pds as a list
        """

        return self.pool.compute(pds)



class BDSRay:
    """
    This is a wrapper for Apache Spark Broadcast variables.
    """

    def __init__(self, bcv):
        """
        Parameters
        ----------
        bcv: rat object storage var
            Initialize with a Ray object storage variable
        """

        self.bcv = bcv
        self._value = None


    def value(self):
        """
        Returns
        -------
        object
            returns the referenced object that was broadcasted.
        """
        if self._value is None:
            self._value = ray.get(self.bcv)
        return self._value


