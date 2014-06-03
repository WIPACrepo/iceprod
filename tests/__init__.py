import unittest

# override multiprocessing to get coverage to work
import multiprocessing
class CoverageProcess(multiprocessing.Process):
    def run(self):
        import coverage
        cov = coverage.coverage(data_suffix=True)
        cov.start()
        super(CoverageProcess,self).run()
        cov.stop()
        cov.save()
multiprocessing.Process = CoverageProcess

def load_suite():
    # accumulate tests
    loader = unittest.defaultTestLoader
    test_suites = loader.discover('iceprod.core.tests','*.py')
    return unittest.TestSuite(test_suites)
