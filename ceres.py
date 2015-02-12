# Copyright 2011 Chris Davis
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#
#

# Ceres requires Python 2.6 or newer
import os
import struct
import json
import errno
import time
from math import isnan
from itertools import izip
from os.path import isdir, exists, join, dirname, abspath, getsize, getmtime
from glob import glob
from bisect import bisect_left


TIMESTAMP_FORMAT = "!L"
TIMESTAMP_SIZE = struct.calcsize(TIMESTAMP_FORMAT)
DATAPOINT_FORMAT = "!d"
DATAPOINT_SIZE = struct.calcsize(DATAPOINT_FORMAT)
NAN = float('nan')
PACKED_NAN = struct.pack(DATAPOINT_FORMAT, NAN)
MAX_SLICE_GAP = 80
DEFAULT_TIMESTEP = 60
DEFAULT_SLICE_CACHING_BEHAVIOR = 'none'
SLICE_PERMS = 0644
DIR_PERMS = 0755


class CeresTree:
  """Represents a tree of Ceres metrics contained within a single path on disk
  This is the primary Ceres API.

  :param root: The directory root of the Ceres tree

  See :func:`setDefaultSliceCachingBehavior` to adjust caching behavior
  """
  def __init__(self, root):
    if isdir(root):
      self.root = abspath(root)
    else:
      raise ValueError("Invalid root directory '%s'" % root)
    self.nodeCache = {}

  def __repr__(self):
    return "<CeresTree[0x%x]: %s>" % (id(self), self.root)
  __str__ = __repr__

  @classmethod
  def createTree(cls, root, **props):
    """Create and returns a new Ceres tree with the given properties

    :param root: The root directory of the new Ceres tree
    :keyword \*\*props: Arbitrary key-value properties to store as tree metadata

    :returns: :class:`CeresTree`
    """

    ceresDir = join(root, '.ceres-tree')
    if not isdir(ceresDir):
      os.makedirs(ceresDir, DIR_PERMS)

    for prop,value in props.items():
      propFile = join(ceresDir, prop)
      fh = open(propFile, 'w')
      fh.write(str(value))
      fh.close()

    return cls(root)

  def walk(self, **kwargs):
    """Iterate through the nodes contained in this :class:`CeresTree`

      :keyword \*\*kwargs: Options to pass to `os.walk`

      :returns: An iterator yielding :class:`CeresNode` objects
    """
    for (fsPath, subdirs, filenames) in os.walk(self.root, **kwargs):
      if CeresNode.isNodeDir(fsPath):
        nodePath = self.getNodePath(fsPath)
        yield CeresNode(self, nodePath, fsPath)

  def getFilesystemPath(self, nodePath):
    """Get the on-disk path of a Ceres node given a metric name"""
    return join(self.root, nodePath.replace('.', os.sep))

  def getNodePath(self, fsPath):
    """Get the metric name of a Ceres node given the on-disk path"""
    fsPath = abspath(fsPath)
    if not fsPath.startswith(self.root):
      raise ValueError("path '%s' not beneath tree root '%s'" % (fsPath, self.root))

    nodePath = fsPath[len(self.root):].strip(os.sep).replace(os.sep, '.')
    return nodePath

  def hasNode(self, nodePath):
    """Returns whether the Ceres tree contains the given metric"""
    return isdir(self.getFilesystemPath(nodePath))

  def getNode(self, nodePath):
    """Returns a Ceres node given a metric name

      :param nodePath: A metric name

      :returns: :class:`CeresNode` or `None`
    """
    if nodePath not in self.nodeCache:
      fsPath = self.getFilesystemPath(nodePath)
      if CeresNode.isNodeDir(fsPath):
        self.nodeCache[nodePath] = CeresNode(self, nodePath, fsPath)
      else:
        return None

    return self.nodeCache[nodePath]

  def find(self, nodePattern, fromTime=None, untilTime=None):
    """Find nodes which match a wildcard pattern, optionally filtering on
    a time range

      :keyword nodePattern: A glob-style metric wildcard
      :keyword fromTime: Optional interval start time in unix-epoch.
      :keyword untilTime: Optional interval end time in unix-epoch.

      :returns: An iterator yielding :class:`CeresNode` objects
    """
    for fsPath in glob(self.getFilesystemPath(nodePattern)):
      if CeresNode.isNodeDir(fsPath):
        nodePath = self.getNodePath(fsPath)
        node = self.getNode(nodePath)

        if fromTime is None and untilTime is None:
          yield node
        elif node.hasDataForInterval(fromTime, untilTime):
          yield node

  def createNode(self, nodePath, **properties):
    """Creates a new metric given a new metric name and optional per-node metadata
      :keyword nodePath: The new metric name.
      :keyword \*\*properties: Arbitrary key-value properties to store as metric metadata.

      :returns: :class:`CeresNode`
    """
    return CeresNode.create(self, nodePath, **properties)

  def store(self, nodePath, datapoints):
    """Store a list of datapoints associated with a metric
      :keyword nodePath: The metric name to write to
      :keyword datapoints: A list of datapoint tuples: (timestamp, value)
    """
    node = self.getNode(nodePath)

    if node is None:
      raise NodeNotFound("The node '%s' does not exist in this tree" % nodePath)

    node.write(datapoints)

  def fetch(self, nodePath, fromTime, untilTime):
    """Fetch data within a given interval from the given metric

      :keyword nodePath: The metric name to fetch from
      :keyword fromTime: Requested interval start time in unix-epoch.
      :keyword untilTime: Requested interval end time in unix-epoch.

      :returns: :class:`TimeSeriesData`
      :raises: :class:`NodeNotFound`, :class:`InvalidRequest`, :class:`NoData`
    """
    node = self.getNode(nodePath)

    if not node:
      raise NodeNotFound("the node '%s' does not exist in this tree" % nodePath)

    return node.read(fromTime, untilTime)


class CeresNode(object):
  __slots__ = ('tree', 'nodePath', 'fsPath',
               'metadataFile', 'timeStep',
               'sliceCache', 'sliceCachingBehavior')

  def __init__(self, tree, nodePath, fsPath):
    self.tree = tree
    self.nodePath = nodePath
    self.fsPath = fsPath
    self.metadataFile = join(fsPath, '.ceres-node')
    self.timeStep = None
    self.sliceCache = None
    self.sliceCachingBehavior = DEFAULT_SLICE_CACHING_BEHAVIOR

  def __repr__(self):
    return "<CeresNode[0x%x]: %s>" % (id(self), self.nodePath)
  __str__ = __repr__

  @classmethod
  def create(cls, tree, nodePath, **properties):
    # Create the node directory
    fsPath = tree.getFilesystemPath(nodePath)
    os.makedirs(fsPath, DIR_PERMS)

    # Create the initial metadata
    timeStep = properties['timeStep'] = properties.get('timeStep', DEFAULT_TIMESTEP)
    node = cls(tree, nodePath, fsPath)
    node.writeMetadata(properties)

    # Create the initial data file
    #now = int( time.time() )
    #baseTime = now - (now % timeStep)
    #slice = CeresSlice.create(node, baseTime, timeStep)

    return node

  @staticmethod
  def isNodeDir(path):
    return isdir(path) and exists(join(path, '.ceres-node'))

  @classmethod
  def fromFilesystemPath(cls, fsPath):
    dirPath = dirname(fsPath)

    while True:
      ceresDir = join(dirPath, '.ceres-tree')
      if isdir(ceresDir):
        tree = CeresTree(dirPath)
        nodePath = tree.getNodePath(fsPath)
        return cls(tree, nodePath, fsPath)

      dirPath = dirname(dirPath)

      if dirPath == '/':
        raise ValueError("the path '%s' is not in a ceres tree" % fsPath)

  @property
  def slice_info(self):
    return [(slice.startTime, slice.endTime, slice.timeStep) for slice in self.slices]

  def readMetadata(self):
    metadata = json.load(open(self.metadataFile, 'r'))
    self.timeStep = int(metadata['timeStep'])
    return metadata

  def writeMetadata(self, metadata):
    self.timeStep = int(metadata['timeStep'])

    f = open(self.metadataFile, 'w')
    json.dump(metadata, f)
    f.close()

  @property
  def slices(self):
    if self.sliceCache:
      if self.sliceCachingBehavior == 'all':
        for slice in self.sliceCache:
          yield slice

      elif self.sliceCachingBehavior == 'latest':
        yield self.sliceCache
        infos = self.readSlices()
        for info in infos[1:]:
          yield CeresSlice(self, *info)

    else:
      if self.sliceCachingBehavior == 'all':
        self.sliceCache = [CeresSlice(self, *info) for info in self.readSlices()]
        for slice in self.sliceCache:
          yield slice

      elif self.sliceCachingBehavior == 'latest':
        infos = self.readSlices()
        if infos:
          self.sliceCache = CeresSlice(self, *infos[0])
          yield self.sliceCache

        for info in infos[1:]:
          yield CeresSlice(self, *info)

      elif self.sliceCachingBehavior == 'none':
        for info in self.readSlices():
          yield CeresSlice(self, *info)

      else:
        raise ValueError("invalid caching behavior configured '%s'" % self.sliceCachingBehavior)

  def readSlices(self):
    if not exists(self.fsPath):
      raise NodeDeleted()

    slice_info = []
    for filename in os.listdir(self.fsPath):
      if filename.endswith('.slice'):
        startTime, timeStep = filename[:-6].split('@')
        slice_info.append((int(startTime), int(timeStep)))

    slice_info.sort(reverse=True)
    return slice_info

  def setSliceCachingBehavior(self, behavior):
    behavior = behavior.lower()
    if behavior not in ('none', 'all', 'latest'):
      raise ValueError("invalid caching behavior '%s'" % behavior)

    self.sliceCachingBehavior = behavior
    self.sliceCache = None

  def clearSliceCache(self):
    self.sliceCache = None

  def hasDataForInterval(self, fromTime, untilTime):
    slices = list(self.slices)
    if not slices:
      return False

    earliestData = slices[-1].startTime
    latestData = slices[0].endTime

    return ((fromTime is 0) or (fromTime is None) or (fromTime < latestData)) and \
           ((untilTime is 0) or (untilTime is None) or (untilTime > earliestData))

  def read(self, fromTime, untilTime):
    # get biggest timeStep 
    metadata = None
    if self.timeStep is None:
      metadata = self.readMetadata()

    # Normalize the timestamps to fit proper intervals
    fromTime = int(fromTime - (fromTime % self.timeStep))
    untilTime = int(untilTime - (untilTime % self.timeStep))

    sliceBoundary = None  # to know when to split up queries across slices
    resultValues = []
    earliestData = None

    # calculate biggest timeStep in slices with data in requested period
    biggest_timeStep = 1
    slices_map = {}
    for slice_tmp in self.slices:
      slices_map[slice_tmp.fsPath] = [slice_tmp.startTime, slice_tmp.endTime, slice_tmp.timeStep]
      if fromTime >= slice_tmp.startTime:
        if biggest_timeStep < slice_tmp.timeStep: biggest_timeStep = slice_tmp.timeStep
        break
      elif untilTime >= slice_tmp.startTime:
        if biggest_timeStep < slice_tmp.timeStep: biggest_timeStep = slice_tmp.timeStep

    resultValues = None
    result_length = 0

    slices_arr = []
    for slice_tmp in self.slices:
      bogus = 0
      for item in slices_map.values():
        if (slice_tmp.startTime > item[0] and slice_tmp.endTime < item[1]) or (slice_tmp.startTime > untilTime or slice_tmp.endTime < fromTime):
          bogus = 1
      if not bogus:
        slices_arr.append(slice_tmp)

      for slice in slices_arr:
        # print("slice timestep=%s start=%s end=%s" % (slice.timeStep, slice.startTime, slice.endTime))
        # if the requested interval starts after the start of this slice
        is_last = False
        if fromTime >= slice.startTime:
          requestUntilTime = untilTime
          requestFromTime = fromTime
          is_last = True
        elif untilTime >= slice.startTime:
          # Or if slice contains data for part of the requested interval...
          # Split the request up if it straddles a slice boundary
          if (sliceBoundary is not None) and untilTime > sliceBoundary:
            requestUntilTime = sliceBoundary
          else:
            requestUntilTime = untilTime
          requestFromTime = slice.startTime
          sliceBoundary = slice.startTime
        else:
          # this is the right-side boundary on the next iteration
          sliceBoundary = slice.startTime
          continue

        try:
          series = slice.read(requestFromTime, requestUntilTime)
          if slice.timeStep < biggest_timeStep:
            series.values = recalculateSeries(series.values, slice.timeStep, biggest_timeStep)
            series.timeStep = biggest_timeStep
          # print("0 slice_len=%s, calculated_len=%s" % (len(series.values), (series.endTime - series.startTime)/biggest_timeStep))
        except NoData:
          break

        earliestData = series.startTime

        rightMissing = (requestUntilTime - series.endTime) / biggest_timeStep

        if resultValues is None:
          result_length = 0

        rightNulls = TimeSeriesData(series.endTime,
                                    series.endTime + rightMissing,
                                    biggest_timeStep,
                                    [None for i in range(rightMissing - result_length)])
        series += rightNulls
        if resultValues is None:
          resultValues = series
        else:
          if resultValues.startTime > series.startTime:
            series.merge(resultValues)
            resultValues = series
          else:
            resultValues.merge(series)
        result_length = len(resultValues)
        if is_last:
          break

    # The end of the requested interval predates all slices
    if earliestData is None:
      if biggest_timeStep is 1:
        now = int(time.time())
        try:
          biggest_timeStep = metadata["timeStep"]
          tmp = 0
          for ts in metadata["retentions"]:
            tmp += ts[0] * ts[1]
            if untilTime > now - tmp:
              break
            biggest_timeStep = ts[0]
        except TypeError:
          biggest_timeStep = DEFAULT_TIMESTEP
      missing = int(untilTime - fromTime) / biggest_timeStep
      resultValues = TimeSeriesData(fromTime, untilTime, biggest_timeStep, [None for i in range(missing)])

    # Left pad nulls if the start of the requested interval predates all slices
    else:
      leftMissing = (earliestData - fromTime) / biggest_timeStep
      leftNulls = TimeSeriesData(fromTime,
                                 fromTime + leftMissing,
                                 biggest_timeStep,
                                 [None for i in range(leftMissing)])
      resultValues = leftNulls + resultValues
    # print("vals=%s, computed_vals=%s" % (len(resultValues.values), (untilTime - fromTime)/biggest_timeStep))
    return resultValues

  def write(self, datapoints):
    if self.timeStep is None:
      self.readMetadata()

    if not datapoints:
      return

    sequences = self.compact(datapoints)
    needsEarlierSlice = []  # keep track of sequences that precede all existing slices

    while sequences:
      sequence = sequences.pop()
      timestamps = [t for t,v in sequence]
      beginningTime = timestamps[0]
      endingTime = timestamps[-1]
      sliceBoundary = None  # used to prevent writing sequences across slice boundaries
      slicesExist = False

      for slice in self.slices:
        if slice.timeStep != self.timeStep:
          continue

        slicesExist = True

        # truncate sequence so it doesn't cross the slice boundaries
        if beginningTime >= slice.startTime:
          if sliceBoundary is None:
            sequenceWithinSlice = sequence
          else:
            # index of highest timestamp that doesn't exceed sliceBoundary
            boundaryIndex = bisect_left(timestamps, sliceBoundary)
            sequenceWithinSlice = sequence[:boundaryIndex]

          try:
            slice.write(sequenceWithinSlice)
          except SliceGapTooLarge:
            newSlice = CeresSlice.create(self, beginningTime, slice.timeStep)
            newSlice.write(sequenceWithinSlice)
            self.sliceCache = None
          except SliceDeleted:
            self.sliceCache = None
            self.write(datapoints)  # recurse to retry
            return

          break

        # sequence straddles the current slice, write the right side
        elif endingTime >= slice.startTime:
          # index of lowest timestamp that doesn't preceed slice.startTime
          boundaryIndex = bisect_left(timestamps, slice.startTime)
          sequenceWithinSlice = sequence[boundaryIndex:]
          leftover = sequence[:boundaryIndex]
          sequences.append(leftover)
          slice.write(sequenceWithinSlice)
          break

        else:
          needsEarlierSlice.append(sequence)

        sliceBoundary = slice.startTime

      if not slicesExist:
        sequences.append(sequence)
        needsEarlierSlice = sequences
        break

    for sequence in needsEarlierSlice:
      slice = CeresSlice.create(self, int(sequence[0][0]), self.timeStep)
      slice.write(sequence)
      self.sliceCache = None

  def compact(self, datapoints):
    datapoints = sorted((int(timestamp), float(value))
                         for timestamp, value in datapoints
                         if value is not None)
    sequences = []
    sequence = []
    minimumTimestamp = 0  # used to avoid duplicate intervals

    for timestamp, value in datapoints:
      timestamp -= timestamp % self.timeStep  # round it down to a proper interval

      if not sequence:
        sequence.append((timestamp, value))

      else:
        if not timestamp > minimumTimestamp:  # drop duplicate intervals
          continue

        if timestamp == sequence[-1][0] + self.timeStep:  # append contiguous datapoints
          sequence.append((timestamp, value))

        else:  # start a new sequence if not contiguous
          sequences.append(sequence)
          sequence = [(timestamp, value)]

      minimumTimestamp = timestamp

    if sequence:
      sequences.append(sequence)

    return sequences


class CeresSlice(object):
  __slots__ = ('node', 'startTime', 'timeStep', 'fsPath')

  def __init__(self, node, startTime, timeStep):
    self.node = node
    self.startTime = startTime
    self.timeStep = timeStep
    self.fsPath = join(node.fsPath, '%d@%d.slice' % (startTime, timeStep))

  def __repr__(self):
    return "<CeresSlice[0x%x]: %s>" % (id(self), self.fsPath)
  __str__ = __repr__

  @property
  def isEmpty(self):
    return getsize(self.fsPath) == 0

  @property
  def endTime(self):
    return self.startTime + ((getsize(self.fsPath) / DATAPOINT_SIZE) * self.timeStep)

  @property
  def mtime(self):
    return getmtime(self.fsPath)

  @classmethod
  def create(cls, node, startTime, timeStep):
    slice = cls(node, startTime, timeStep)
    fileHandle = open(slice.fsPath, 'wb')
    fileHandle.close()
    os.chmod(slice.fsPath, SLICE_PERMS)
    return slice

  def read(self, fromTime, untilTime):
    timeOffset = int(fromTime) - self.startTime

    if timeOffset < 0:
      raise InvalidRequest("requested time range (%d, %d) preceeds this slice: %d" % (fromTime, untilTime, self.startTime))

    pointOffset = timeOffset / self.timeStep
    byteOffset = pointOffset * DATAPOINT_SIZE

    if byteOffset >= getsize(self.fsPath):
      raise NoData()

    fileHandle = open(self.fsPath, 'rb')
    fileHandle.seek(byteOffset)

    timeRange = int(untilTime - fromTime)
    pointRange = timeRange / self.timeStep
    byteRange = pointRange * DATAPOINT_SIZE
    packedValues = fileHandle.read(byteRange)

    pointsReturned = len(packedValues) / DATAPOINT_SIZE
    format = '!' + ('d' * pointsReturned)
    values = struct.unpack(format, packedValues)
    values = [v if not isnan(v) else None for v in values]

    endTime = fromTime + (len(values) * self.timeStep)
    #print '[DEBUG slice.read] startTime=%s fromTime=%s untilTime=%s' % (self.startTime, fromTime, untilTime)
    #print '[DEBUG slice.read] timeInfo = (%s, %s, %s)' % (fromTime, endTime, self.timeStep)
    #print '[DEBUG slice.read] values = %s' % str(values)
    return TimeSeriesData(fromTime, endTime, self.timeStep, values)

  def write(self, sequence):
    beginningTime = sequence[0][0]
    timeOffset = beginningTime - self.startTime
    pointOffset = timeOffset / self.timeStep
    byteOffset = pointOffset * DATAPOINT_SIZE

    values = [v for t,v in sequence]
    format = '!' + ('d' * len(values))
    packedValues = struct.pack(format, *values)

    try:
      filesize = getsize(self.fsPath)
    except OSError, e:
      if e.errno == errno.ENOENT:
        raise SliceDeleted()
      else:
        raise

    byteGap = byteOffset - filesize
    if byteGap > 0:  # pad the allowable gap with nan's
      pointGap = byteGap / DATAPOINT_SIZE
      if pointGap > MAX_SLICE_GAP:
        raise SliceGapTooLarge()
      else:
        packedGap = PACKED_NAN * pointGap
        packedValues = packedGap + packedValues
        byteOffset -= byteGap

    with file(self.fsPath, 'r+b') as fileHandle:
      try:
        fileHandle.seek(byteOffset)
      except IOError:
        print " IOError: fsPath=%s byteOffset=%d size=%d sequence=%s" % (self.fsPath, byteOffset, filesize, sequence)
        raise
      fileHandle.write(packedValues)

  def deleteBefore(self, t):
    if not exists(self.fsPath):
      raise SliceDeleted()

    if t % self.timeStep != 0:
      t = t - (t % self.timeStep) + self.timeStep
    timeOffset = t - self.startTime
    if timeOffset < 0:
      return

    pointOffset = timeOffset / self.timeStep
    byteOffset = pointOffset * DATAPOINT_SIZE
    if not byteOffset:
      return

    self.node.clearSliceCache()
    with file(self.fsPath, 'r+b') as fileHandle:
      fileHandle.seek(byteOffset)
      fileData = fileHandle.read()
      if fileData:
        fileHandle.seek(0)
        fileHandle.write(fileData)
        fileHandle.truncate()
        fileHandle.close()
        newFsPath = join(dirname(self.fsPath), "%d@%d.slice" % (t, self.timeStep))
        os.rename(self.fsPath, newFsPath)
      else:
        os.unlink(self.fsPath)
        raise SliceDeleted()

  def __cmp__(self, other):
    return cmp(self.startTime, other.startTime)


class TimeSeriesData(object):
  __slots__ = ('startTime', 'endTime', 'timeStep', 'values')

  def __init__(self, startTime, endTime, timeStep, values):
    self.startTime = startTime
    self.endTime = endTime
    self.timeStep = timeStep
    self.values = values

  @property
  def timestamps(self):
    return xrange(self.startTime, self.endTime, self.timeStep)

  def __iter__(self):
    return izip(self.timestamps, self.values)

  def __len__(self):
    return len(self.values)

  def __add__(self, other):
    if self.timeStep != other.timeStep:
      raise ValueError("Can't sum data with different timestamps. Mine is %s, other's is %s" %
                         (self.timeStep, other.timeStep))
    new_data = TimeSeriesData(self.startTime, other.endTime, self.timeStep, self.values + other.values)
    return new_data

  def merge(self, other):
    """
    Merge two TimeSeriesData objects together

    :param other: another TimeSeriesData object, that'll be merged. Note, other.startTime must be greater than self's.
    :return: Nothing
    """
    if self.timeStep != other.timeStep:
        raise ValueError("Can't merge data with different timestamps. Mine is %s, other's is %s" %
                           (self.timeStep, other.timeStep))
    # Align timestamp
    ts = other.startTime - (other.startTime % self.timeStep)
    index = int((ts - self.startTime) / self.timeStep)
    for value in other.values:
      # Adjust timestamp to be aligned on timeStep boundary.
      if ts > self.endTime:
        self.values.append(value)
      else:
        try:
          self.values[index] = value
        except IndexError:
          self.values.append(value)
        index += 1
      ts += self.timeStep
    if other.endTime > self.endTime:
      self.endTime = other.endTime


class CorruptNode(Exception):
  def __init__(self, node, problem):
    Exception.__init__(self, problem)
    self.node = node
    self.problem = problem


class NoData(Exception):
  pass


class NodeNotFound(Exception):
  pass


class NodeDeleted(Exception):
  pass


class InvalidRequest(Exception):
  pass


class SliceGapTooLarge(Exception):
  "For internal use only"


class SliceDeleted(Exception):
  pass

def aggregate_avg(values):
    """
    Compute AVG for list of points.
    :param values: list of values
    :return:
    """
    length = len(values)
    if length is 0:
        return None
    length_iter = range(length)
    s = 0
    nones = 0
    for i in length_iter:
        if values[i] is None:
            length -= 1
            nones += 1
            if nones > length:
                return None
        else:
            s += values[i]
    agg = float(s) / length
    return agg

def recalculateSeries(values, old_timeStep, new_timeStep):
    """
    Recalculate values to the new timeStep.
    :param values: list of the values
    :param old_timeStep: previous timestep
    :param new_timeStep: new timeStep value
    :return: list of recalculated values
    """
    factor = int(new_timeStep/old_timeStep)

    new_values = list()
    sub_arr = list()
    cnt = 0
    for i in range(0, len(values)):
        sub_arr.append(values[i])
        cnt += 1
        if cnt == factor:
                new_values.append(aggregate_avg(sub_arr))
                sub_arr = list()
                cnt = 0
    if len(sub_arr) > int(factor/4):
        new_values.append(aggregate_avg(sub_arr))
    return new_values


def getTree(path):
  while path not in (os.sep, ''):
    if isdir(join(path, '.ceres-tree')):
      return CeresTree(path)

    path = dirname(path)


def setDefaultSliceCachingBehavior(behavior):
  global DEFAULT_SLICE_CACHING_BEHAVIOR

  behavior = behavior.lower()
  if behavior not in ('none', 'all', 'latest'):
    raise ValueError("invalid caching behavior '%s'" % behavior)

  DEFAULT_SLICE_CACHING_BEHAVIOR = behavior
