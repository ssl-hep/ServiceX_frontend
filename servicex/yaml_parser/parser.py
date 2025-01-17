# Original code released under
# The MIT License (MIT)
#
#  Copyright (c) 2014-2018 Tristan Sweeney, Cambridge Consultants
#
#  Permission is hereby granted, free of charge, to any person obtaining a copy
#  of this software and associated documentation files (the "Software"), to deal
#  in the Software without restriction, including without limitation the rights
#  to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
#  copies of the Software, and to permit persons to whom the Software is
#  furnished to do so, subject to the following conditions:
#
#  The above copyright notice and this permission notice shall be included in
#  all copies or substantial portions of the Software.
#
#  THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
#  IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
#  FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
#  AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
#  LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
#  OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
#  SOFTWARE.
#
# The code has been modified:
# Copyright (c) 2024, IRIS-HEP
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

import types
from typing import Union, Any, Protocol
from pathlib import Path
import os

import ruamel.yaml
import ruamel.yaml.composer
import ruamel.yaml.constructor
from ruamel.yaml.nodes import ScalarNode, MappingNode, SequenceNode


class TextFileLike(Protocol):
    def read(self, size: int) -> Union[str, bytes]:
        """read function for a file-like object"""


class CompositingComposer(ruamel.yaml.composer.Composer):
    compositors = {k: {} for k in (ScalarNode, MappingNode, SequenceNode)}

    @classmethod
    def add_compositor(cls, tag, compositor, *, nodeTypes=(ScalarNode,)):
        for nodeType in nodeTypes:
            cls.compositors[nodeType][tag] = compositor

    @classmethod
    def get_compositor(cls, tag, nodeType):
        return cls.compositors[nodeType].get(tag, None)

    def __compose_dispatch(self, anchor, nodeType, callback):
        event = self.parser.peek_event()
        compositor = self.get_compositor(event.tag, nodeType) or callback
        if isinstance(compositor, types.MethodType):
            return compositor(anchor)
        else:
            return compositor(self, anchor)

    def compose_scalar_node(self, anchor):
        return self.__compose_dispatch(anchor, ScalarNode, super().compose_scalar_node)

    def compose_sequence_node(self, anchor):
        return self.__compose_dispatch(
            anchor, SequenceNode, super().compose_sequence_node
        )

    def compose_mapping_node(self, anchor):
        return self.__compose_dispatch(
            anchor, MappingNode, super().compose_mapping_node
        )


class ExcludingConstructor(ruamel.yaml.constructor.Constructor):
    filters = {k: [] for k in (MappingNode, SequenceNode)}

    @classmethod
    def add_filter(cls, filter, *, nodeTypes=(MappingNode,)):
        for nodeType in nodeTypes:
            cls.filters[nodeType].append(filter)

    def construct_mapping(self, node):
        node.value = [
            (key_node, value_node)
            for key_node, value_node in node.value
            if not any(f(key_node, value_node) for f in self.filters[MappingNode])
        ]
        return super().construct_mapping(node)

    def construct_sequence(self, node, deep=True):
        node.value = [
            value_node
            for value_node in node.value
            if not any(f(value_node) for f in self.filters[SequenceNode])
        ]
        return super().construct_sequence(node)


class YAML(ruamel.yaml.YAML):
    def __init__(self, *args, **kwargs):
        if "typ" not in kwargs:
            kwargs["typ"] = "safe"
        elif kwargs["typ"] not in ("safe", "unsafe") and kwargs["typ"] not in (
            ["safe"],
            ["unsafe"],
        ):  # pragma: no cover
            raise Exception(
                "Can't do typ={} parsing w/ composition time directives!".format(
                    kwargs["typ"]
                )
            )

        if "pure" not in kwargs:
            kwargs["pure"] = True
        elif not kwargs["pure"]:  # pragma: no cover
            raise Exception(
                "Can't do non-pure python parsing w/ composition time directives!"
            )

        super().__init__(*args, **kwargs)
        self.Composer = CompositingComposer
        self.Constructor = ExcludingConstructor

    def compose(self, stream: Union[Path, str, bytes, TextFileLike]) -> Any:
        """
        at this point you either have the non-pure Parser (which has its own reader and
        scanner) or you have the pure Parser.
        If the pure Parser is set, then set the Reader and Scanner, if not already set.
        If either the Scanner or Reader are set, you cannot use the non-pure Parser,
            so reset it to the pure parser and set the Reader resp. Scanner if necessary
        """
        constructor, parser = self.get_constructor_parser(stream)
        try:
            return self.composer.get_single_node()
        finally:
            parser.dispose()
            try:
                self._reader.reset_reader()
            except AttributeError:  # pragma: no cover
                pass
            try:
                self._scanner.reset_scanner()
            except AttributeError:  # pragma: no cover
                pass

    def fork(self):
        return type(self)(typ=self.typ, pure=self.pure)


def include_compositor(self, anchor):
    event = self.parser.get_event()
    yaml = self.loader.fork()
    path = os.path.join(os.path.dirname(self.loader.reader.name), event.value)
    with open(os.path.abspath(path)) as f:
        rv = yaml.compose(f)
        self.loader.composer.anchors.update(yaml.composer.anchors)
        return rv


def exclude_filter(key_node, value_node=None):
    value_node = value_node or key_node  # copy ref if None
    return key_node.tag == "!exclude" or value_node.tag == "!exclude"


CompositingComposer.add_compositor("!include", include_compositor)
ExcludingConstructor.add_filter(exclude_filter, nodeTypes=(MappingNode, SequenceNode))
