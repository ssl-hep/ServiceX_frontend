# Tests against a ServiceX instance. This is not meant to be a complete
# test of every code path. Just a sanity check. The unit tests are meant to
# do that sort of testing.
# An example endpoint (pass as arg to this script):
#       http://localhost:5000
import sys
from servicex import ServiceXDataset
from servicex.servicex_adaptor import ServiceXAdaptor
from typing import Optional


def run_query(endpoint: Optional[ServiceXAdaptor]) -> None:
    ds = ServiceXDataset(
        "mc15_13TeV:mc15_13TeV.361106.PowhegPythia8EvtGen_AZNLOCTEQ6L1_Zee.merge.DAOD_STDM3.e3601_s2576_s2132_r6630_r6264_p2363_tid05630052_00",  # NOQA
        servicex_adaptor=endpoint)

    r = ds.get_data_rootfiles("(call ResultTTree (call Select (call SelectMany (call EventDataset (list 'localds:bogus')) (lambda (list e) (call (attr e 'Jets') 'AntiKt4EMTopoJets'))) (lambda (list j) (/ (call (attr j 'pt')) 1000.0))) (list 'JetPt') 'analysis' 'junk.root')")  # NOQA
    print(r)


if __name__ == '__main__':
    servicex_adaptor = ServiceXAdaptor(sys.argv[1]) if len(sys.argv) >= 2 else None
    run_query(servicex_adaptor)
