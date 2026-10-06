"""Small analytical fixtures that test the estimator, not its implementation shape."""
import unittest
import numpy as np
from scipy.sparse import csr_matrix
from scipy.sparse.csgraph import dijkstra
from pipeline import kernel,time_matrix,spatial_matrix,legacy_calculation,H

class EstimatorTests(unittest.TestCase):
    def test_kernel_normalization_and_support(self):
        x=np.linspace(-1,1,10001)
        for kind in ['epanechnikov','quartic']:
            self.assertAlmostEqual(float(np.trapezoid(kernel(x,1,kind),x)),1,places=7)
            np.testing.assert_array_equal(kernel([100,101,200],100,kind),[0,0,0])
    def test_reject_invalid_bandwidth_and_kernel(self):
        for bandwidth in [0,-1,np.nan,np.inf]:
            with self.assertRaises(ValueError):kernel(0,bandwidth)
        with self.assertRaises(ValueError):kernel(0,100,'typo')
    def test_calendar_gap_is_not_compressed(self):
        _,delta=time_matrix([2012,2014,2017],2,False)
        self.assertEqual(delta[0,1],2)
        self.assertEqual(delta[1,2],3)
    def test_midnight_wrap_and_kernel_boundary(self):
        temporal,delta=time_matrix(range(24),2,True)
        self.assertEqual(delta[23,0],1)
        self.assertGreater(temporal[23,0],0)
        self.assertEqual(temporal[22,0],0)
    def test_spatial_decay_and_formula_isolation(self):
        distances=csr_matrix(np.array([[0.,10.,90.],[10.,0.,80.],[90.,80.,0.]]))
        distances.setdiag(0)
        counts=np.array([[1.],[0.],[0.]])
        baseline=legacy_calculation(counts,distances,[2014],1,False)
        formula=legacy_calculation(counts,distances,[2014],1,False,True)
        self.assertAlmostEqual(baseline[1,0],baseline[2,0])
        self.assertGreater(formula[1,0],formula[2,0])
        self.assertAlmostEqual(formula[2,0]/baseline[2,0],.19)
    def test_source_lixel_temporal_smoothing_and_zero_count_output(self):
        distances=csr_matrix(([0.],([0],[0])),shape=(1,1))
        counts=np.zeros((1,24));counts[0,8]=4
        temporal,_=time_matrix(range(24),2,True)
        corrected=spatial_matrix(distances)@counts@temporal
        original=legacy_calculation(counts,distances,range(24),2,True)
        self.assertEqual(original[0,9],0)
        self.assertGreater(corrected[0,9],0)
        self.assertEqual(counts[0,9],0)
        self.assertEqual(corrected[0,10],0)
    def test_matrix_estimator_against_hand_sum(self):
        distances=csr_matrix(([0,30,30,0,0],([0,0,1,1,2],[0,1,0,1,2])),shape=(3,3))
        counts=np.array([[0,2,0],[1,0,3],[4,0,0.]])
        bins=[2012,2013,2014];T=2
        temporal,_=time_matrix(bins,T,False)
        actual=spatial_matrix(distances)@counts@temporal
        expected=np.zeros_like(counts)
        for target in range(3):
            for time_index,time in enumerate(bins):
                for source in range(3):
                    row=distances.getrow(target)
                    if source not in row.indices:continue
                    distance=float(row[0,source])
                    for source_time_index,source_time in enumerate(bins):
                        expected[target,time_index]+=counts[source,source_time_index]*kernel(distance,H)/H*kernel(abs(time-source_time),T)/T
        np.testing.assert_allclose(actual,expected,rtol=1e-14)
        self.assertEqual(actual[2,2],0) # disconnected roads never exchange intensity
    def test_bounded_routes_follow_network_not_straight_line(self):
        # Two road midpoints can be nearby in space but far around a U-shaped network.
        graph=csr_matrix(np.array([[0,60,0],[60,0,60],[0,60,0.]]))
        full=dijkstra(graph,directed=False)
        bounded=dijkstra(graph,directed=False,limit=100)
        self.assertEqual(full[0,2],120)
        self.assertTrue(np.isinf(bounded[0,2]))

if __name__=='__main__':unittest.main(verbosity=2)
