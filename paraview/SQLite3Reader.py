#
# CC0 "No rights reserved" - feel free to use anywhere
#

from vtkmodules.vtkCommonDataModel import vtkTable
from paraview.util.vtkAlgorithm import smproxy, smproperty, smdomain
from vtkmodules.util.vtkAlgorithm import VTKPythonAlgorithmBase
from vtk.numpy_interface.dataset_adapter import numpyTovtkDataArray as tovtk
from paraview import vtk
import numpy
from sqlite3 import connect

@smproxy.reader(extensions="sqlite sqlite3",
  file_description="SQLite 3 to VTKTable Reader")
class TrilinkReader(VTKPythonAlgorithmBase):
  def __init__(self):
    VTKPythonAlgorithmBase.__init__(self,
       nInputPorts=0, nOutputPorts=1, outputType='vtkTable')
    self.__filename = None
    self.__query = "select 1 as x"

  @smproperty.stringvector(name="FileName", panel_visibility="never")
  @smdomain.filelist()
  def SetFileName(self, fn):
    if self.__filename != fn:
      self.__filename = fn
      self.Modified()

  @smproperty.stringvector(name="Query", default_values="select 1 as x")
  @smdomain.filelist()
  def SetQuery(self, query):
    if self.__query != query:
      self.__query = query
      self.Modified()

  def RequestData(self, request, ininfo, outinfo):
    out = vtkTable.GetData(outinfo)

    conn = connect(self.__filename)
    cursor = conn.cursor()
    cursor.execute(self.__query)
    row = cursor.fetchone()
    if row is None:
      out.SetNumberOfRows(0)
      return 1
    names = [i[0] for i in cursor.description]

    columns = [[i] for i in row]
    for row in cursor:
      for i, c in zip(row, columns):
        c.append(i)

    if len(columns) > 0:
      out.SetNumberOfRows(len(columns[0]))
    else:
      out.SetNumberOfRows(0)
    for c, n in zip(columns, names):
      out.AddColumn(tovtk(numpy.array(c), n))

    return 1
