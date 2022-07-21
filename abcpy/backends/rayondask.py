from abcpy.backends import Backend, PDS
import ray, dask


import sys
import ray.cloudpickle as pickle
pickle.register_pickle_by_value(sys.modules[__name__])


class BackendRayOnDask(Backend):
    """
    A parallelization backend for Apache Spark. It is essetially a wrapper for
    the required Dask on Ray functionality.
    """

    def __init__(self, n_partitions = None):
        """
        Initialize the backend with an existing and configured SparkContext.

        Parameters
        ----------
        sparkContext: pyspark.SparkContext
            an existing and fully configured PySpark context
        parallelism: int
            defines on how many workers a distributed dataset can be distributed
        """
        self.n_partitions = n_partitions

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
    
        if self.n_partitions:
            rdd = dask.bag.from_sequence(python_list, npartitions = self.n_partitions)
        else:
            rdd = dask.bag.from_sequence(python_list)

        return rdd

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
        new_pds = dask.bag.map(func, pds)
        return new_pds

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

        python_list = pds.compute()
        return python_list



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


    def value(self):
        """
        Returns
        -------
        object
            returns the referenced object that was broadcasted.
        """

        return ray.get(self.bcv)


