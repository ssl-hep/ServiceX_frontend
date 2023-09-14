# Copyright (c) 2022, IRIS-HEP
# All rights reserved.
#
# Redistribution and use in source and binary forms, with or without
# modification, are permitted provided that the following conditions are met:
#
# * Redistributions of source code must retain the above copyright notice, this
#   list of conditions and the following disclaimer.
#
# * Redistributions in binary form must reproduce the above copyright notice,
#   this list of conditions and the following disclaimer in the documentation
#   and/or other materials provided with the distribution.
#
# * Neither the name of the copyright holder nor the names of its
#   contributors may be used to endorse or promote products derived from
#   this software without specific prior written permission.
#
# THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS "AS IS"
# AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE
# IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE ARE
# DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT HOLDER OR CONTRIBUTORS BE LIABLE
# FOR ANY DIRECT, INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR CONSEQUENTIAL
# DAMAGES (INCLUDING, BUT NOT LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR
# SERVICES; LOSS OF USE, DATA, OR PROFITS; OR BUSINESS INTERRUPTION) HOWEVER
# CAUSED AND ON ANY THEORY OF LIABILITY, WHETHER IN CONTRACT, STRICT LIABILITY,
# OR TORT (INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT OF THE USE
# OF THIS SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.
from servicex import ServiceXClient, RucioDatasetIdentifier, ResultFormat
from func_adl_servicex_xaodr22.event_collection import Event

# A Z to ee sample - Release 21
ds_name = r"mc16_13TeV:mc16_13TeV.361106.PowhegPythia8EvtGen_AZNLOCTEQ6L1_Zee.deriv.DAOD_PHYS.e3601_e5984_s3126_r10201_r10210_p5313"

sx = ServiceXClient(backend="testing4")
did = RucioDatasetIdentifier(ds_name, num_files=10)

ds_raw = sx.func_adl_dataset(
    did, codegen="atlasr21", title="Zee", result_format=ResultFormat.parquet, item_type=Event)

from func_adl_servicex_xaodr22 import calib_tools
# ds = calib_tools.apply_calibration(ds_raw, "PHYS") <this is what we should have>
ds = calib_tools.query_update(ds_raw, calib_tools.default_config("PHYSLITE"))

good_ele = ds.Select(
    lambda e: {
        "run": e.EventInfo("EventInfo").runNumber(),
        "event": e.EventInfo("EventInfo").eventNumber(),
        "good_ele": e.Electrons("Electrons").Where(lambda e: (e.pt() / 1000 > 25.0) and (abs(e.eta()) < 2.5)
        ),
    }
)

electron_pt = good_ele.Select(lambda e: {
    "run": e.run,
    "event": e.event,
    "pt": e.good_ele.Select(lambda ele: ele.pt()/1000.0),
})

r = electron_pt.as_signed_urls()
print(f"number of URLs: {len(r.signed_url_list)}")

