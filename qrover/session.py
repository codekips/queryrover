from qRover.mgmt import RoverMgmt
from qRover.qrover import QueryRover


class Session :
    def __init__(self) -> None:
        self._manager = RoverMgmt()
        self._rover = QueryRover("")
        pass
    @property
    def manager(self):
        return self._manager
    @property
    def rover(self):
        return self._rover