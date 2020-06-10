# Tests against a ServiceX instance. This is not meant to be a complete
# test of every code path. Just a sanity check. The unit tests are meant to
# do that sort of testing.
# An example endpoint (pass as arg to this script):
#       http://localhost:5000/servicex
import sys
from servicex import ServiceX


def run_query(endpoint: str) -> None:
    ds = ServiceX(
        "mc15_13TeV:mc15_13TeV.361106.PowhegPythia8EvtGen_AZNLOCTEQ6L1_Zee.merge.DAOD_STDM3.e3601_s2576_s2132_r6630_r6264_p2363_tid05630052_00",  # NOQA
        endpoint)
    r = ds.get_data_rootfiles("(call ResultTTree (call Select (call SelectMany (call EventDataset (list 'localds:bogus')) (lambda (list e) (call (attr e 'Jets') 'AntiKt4EMTopoJets'))) (lambda (list j) (/ (call (attr j 'pt')) 1000.0))) (list 'JetPt') 'analysis' 'junk.root')")  # NOQA
    print(r)


if __name__ == '__main__':
    assert len(sys.argv) == 2
    run_query(sys.argv[1])

# TODO: Make sure files download while other things are still running
# TODO: The total number of files sometimes balloons up to 22 from 17 for this
#       example. Should not happen.
