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
import yaml
import pathlib
from typing import Any, Dict, Union
import rich


def load_databinder_config(input_config:
                           Union[str, pathlib.Path, Dict[str, Any]]
                           ) -> Dict[str, Any]:
    """
    Loads, validates, and returns DataBinder configuration.
    The order of function call matters.
    Args:
        input_config (Union[str, pathlib.Path, Dict[str, Any]]):
            path to config file or config as dict
    Returns:
        Dict[str, Any]: configuration
    """
    def prepare_config(config):
        _set_default_values(config)
        ndef = _replace_definition_in_sample_block(config)
        rich.print(f"Replaced {ndef} Option values from Definition block")
        _support_old_option_names(config)
        _decorate_backend_per_sample(config)
        return _validate_config(config)

    if isinstance(input_config, Dict):
        return prepare_config(input_config)
    else:
        file_path = pathlib.Path(input_config)
        config = yaml.safe_load(file_path.read_text())
        rich.print(f"Loading DataBinder config file: {file_path}")
        return prepare_config(config)


def _set_default_values(config: Dict[str, Any]) -> Dict:
    """
    Default values in General block:
        Delivery: LocalPath
        OutputFormat: root
    """
    if 'Delivery' in config['General'].keys():
        config['General']['Delivery'] = config['General']['Delivery'].lower()
    else:
        config['General']['Delivery'] = 'localpath'

    if 'OutputFormat' not in config['General'].keys():
        config['General']['OutputFormat'] = 'root'

    if 'OutFilesetName' not in config['General'].keys():
        config['General']['OutFilesetName'] = 'servicex_fileset'

    for (idx, sample) in zip(range(len(config['Sample'])), config['Sample']):
        if 'IgnoreLocalCache' not in sample.keys():
            if 'IgnoreLocalCache' in config['General'].keys():
                config['Sample'][idx]['IgnoreLocalCache'] = config['General']['IgnoreLocalCache']
            else:
                config['Sample'][idx]['IgnoreLocalCache'] = False

    return config


def _replace_definition_in_sample_block(config: Dict[str, Any]):
    """
    Replace DEF_X in the Sample block with
    a value of the same DEF_X key in the Definition block
    """
    ndef = 0
    definition = config.get('Definition')
    if definition is not None:
        samples = config.get('Sample')
        if samples is not None:
            for n, sample in enumerate(samples):
                for field, value in sample.items():
                    if isinstance(value, str):
                        if 'DEF_' in value:
                            def_in_value = True
                            for repre, new_str in definition.items():
                                if repre == value:
                                    samples[n][field] \
                                        = samples[n][field] \
                                        .replace(repre, new_str)
                                    ndef = ndef + 1
                                    def_in_value = False
                            if def_in_value:
                                raise NotImplementedError(
                                    f"{value} is NOT defined in the Definition block"
                                )
        return ndef
    else:
        return ndef


def _support_old_option_names(config: Dict[str, Any]) -> Dict:
    """ """
    if 'ServiceXName' in config['General'].keys():
        config['General']['ServiceX'] = config['General'].pop('ServiceXName')

    if 'Transformer' in config['General'].keys():
        config['General']['Codegen'] = config['General'].pop('Transformer')
    for (idx, sample) in zip(range(len(config['Sample'])), config['Sample']):
        if 'Transformer' in sample.keys():
            config['Sample'][idx]['Codegen'] = config['Sample'][idx].pop('Transformer')

    if 'IgnoreServiceXCache' in config['General'].keys():
        config['General']['IgnoreLocalCache'] = config['General'].pop('IgnoreServiceXCache')
    for (idx, sample) in zip(range(len(config['Sample'])), config['Sample']):
        if 'IgnoreServiceXCache' in sample.keys():
            config['Sample'][idx]['IgnoreLocalCache'] = config['Sample'][idx].pop(
                'IgnoreServiceXCache')
    return config


def _decorate_backend_per_sample(config: Dict[str, Any]) -> Dict:
    """ from General block """
    pair = ("default_transformer", "default_codegen")
    if 'Codegen' in config['General'].keys():
        if config['General']['Codegen'] == "atlasr21":
            pair = ("xaod", "atlasr21")
        elif config['General']['Codegen'] == "uproot":
            pair = ("uproot", "uproot")
        elif config['General']['Codegen'] == "python":
            pair = ("uproot", "python")

    """ from Sample block """
    for (idx, sample) in zip(range(len(config['Sample'])), config['Sample']):
        if 'Codegen' in sample.keys():
            if sample['Codegen'] == "atlasr21":
                config['Sample'][idx]['Transformer'] = "xaod"
            elif sample['Codegen'] == "uproot":
                config['Sample'][idx]['Transformer'] = "uproot"
            elif sample['Codegen'] == "python":
                config['Sample'][idx]['Transformer'] = "uproot"
        else:
            config['Sample'][idx]['Transformer'] = pair[0]
            config['Sample'][idx]['Codegen'] = pair[1]

    return config


def _validate_config(config: Dict[str, Any]):
    """Returns True if the config file is validated,
    otherwise raises exceptions.
    Checks that the config satisfies the json schema,
    and performs additional checks to
    validate the config further.
    Args:
        config (Dict[str, Any]): configuration
    Raises:
        NotImplementedError
        ValueError
        KeyError
    Returns:
        bool: whether the validation was successful
    """

    # Option names
    available_keys = [
        'General', 'ServiceX', 'OutputDirectory', 'Transformer', 'Codegen',
        'OutputFormat', 'OutFilesetName', 'IgnoreLocalCache', 'Delivery',
        'Name',
        'Sample', 'RucioDID', 'XRootDFiles', 'Tree',
        'Filter', 'Columns', 'FuncADL', 'LocalPath', 'Definition',
        'Function', 'NFiles'
    ]

    # General and Sample are mandatory blocks
    if 'General' not in config.keys() and 'Sample' not in config.keys():
        raise KeyError("You should have 'General' block and "
                       "at least one 'Sample' block in the config")

    # Check all Opion names
    keys_in_config = set()
    for item in config['General']:
        keys_in_config.add(item)
    for sample in config['Sample']:
        for item in sample.keys():
            keys_in_config.add(item)
    for key in keys_in_config:
        if key not in available_keys:
            raise KeyError(f"Unknown Option {key} in the config")

    # Check General block option values
    if 'Delivery' in config['General'].keys():
        if config['General']['Delivery'] not in [
                'localpath', 'localcache', 'objectstore']:
            raise ValueError(
                f"Unsupported delivery option: {config['General']['Delivery']}"
                f" - supported options: LocalPath, LocalCache, ObjectStore"
            )
    if 'ServiceX' not in config['General'].keys():
        raise KeyError(
            "Option 'ServiceXName' is required in General block"
        )
    if config['General']['OutputFormat'].lower() != 'parquet' and \
            config['General']['OutputFormat'].lower() != 'root':
        raise ValueError(
            "OutputFormat can be either parquet or root"
        )

    # Check Sample block option values
    for sample in config['Sample']:
        if ('RucioDID' not in sample.keys()) \
                and ('XRootDFiles' not in sample.keys()) \
                and ('LocalPath' not in sample.keys()):
            raise KeyError(
                "Please specify a valid input source "
                f"for Sample {sample['Name']} e.g. RucioDID, XRootDFiles"
            )
        if 'RucioDID' in sample.keys():
            for did in sample['RucioDID'].split(","):
                if len(did.split(":")) != 2:
                    raise ValueError(
                        f"Sample {sample['Name']} "
                        f"- RucioDID {did} is missing the scope"
                    )
        if ('Tree' in sample) and \
                ('uproot' not in sample['Transformer']):
            raise KeyError(
                f"Option Tree in Sample {sample['Name']} "
                "is only available for uproot transformer"
            )
        if 'Columns' in sample and 'FuncADL' in sample:
            raise KeyError(
                f"Sample {sample['Name']} - Use one type of query per sample: "
                "Columns for TCut and FuncADL for func-adl"
            )
        if 'FuncADL' in sample and 'Filter' in sample:
            raise KeyError(
                f"Sample {sample['Name']} - "
                "You cannot use Filter with func-adl query"
            )

    return config
