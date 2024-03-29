from typing import TypedDict
from typing_extensions import Unpack
from .datasets.dataset import Dataset
from .datasets.service import MetaDataService


class RoverMgmt(object):
   

	def __init__(self) -> None:
		self.dataset_service:MetaDataService = MetaDataService()

	"""
	This will come in handy, when there are datasets that may need additional parameters to be fetched
	"""
	class _DatasetParams(TypedDict) :
		userId: str
		apiKey: str
	"""
		Register dataset to the system.
		:param location : storage location of the dataset
		:param schema (optional): schema that defines column names and types for the dataset.
		For csv files, this schema would be inferred.
		:param type: format of the passed dataset.
		:param kwargs: any additional args that would be needed to add/ access this dataset.
	"""
	def add_dataset(self, name: str, location: str, type: str='csv', header:list[str]=[], **kwargs: Unpack[_DatasetParams]) -> Dataset:
		match type:
			case 'csv':
				return self.dataset_service.add_csv_dataset(name, location, header)
			case _:
				raise NotImplementedError(f"Dataset type {type} is not supported yet.")
	
