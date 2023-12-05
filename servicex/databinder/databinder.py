# Copyright (c) 2023, IRIS-HEP
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
from typing import Union, Dict, Any
from pathlib import Path
import asyncio

from servicex.databinder.databinder_configuration import load_databinder_config
from servicex.databinder.databinder_deliver import DataBinderDeliver


class DataBinder:
    """

    """
    def __init__(self, config: Union[str, Path, Dict[str, Any]]):
        self._config = load_databinder_config(config)
        self._sx_ds = DataBinderDeliver(self._config)

    def deliver(self, overall_progress_only: bool = False):
        out_paths_dict = asyncio.run(self._sx_ds.get_data())

        # x = Thread(target=OutputHandler(self._config)
        #            .clean_up_files_not_in_requests, args=(out_paths_dict,))
        # x.start()

        # if len(self._sx_db.failed_request):
        #     log.warning(f"{len(self._sx_db.failed_request)} "
        #                 "failed delivery request(s)")
        #     log.warning("get_failed_requests() for detail of failed requests")

        return out_paths_dict

    # def get_failed_requests(self):
    #     return self._sx_db.failed_request
