import abc
from enum import Enum
from qRover.datasets.dataset import Dataset

from qRover.query.parser import AttributeParser

class Qualifiable(abc.ABC):
    @abc.abstractmethod
    def qualify(self, mapping: dict[str, Dataset]) -> None:
        raise NotImplementedError()

class ComparisonOperator(Enum):
    gt = '>'
    lt = '<'
    ge = '>='
    eq = '=='
    le = '<='
    neq = '!='
    @staticmethod
    def from_str(op: str):
        match op:
            case '>':return ComparisonOperator.gt
            case '<':return ComparisonOperator.lt
            case '>=':return ComparisonOperator.ge
            case '==':return ComparisonOperator.eq
            case '<=':return ComparisonOperator.le
            case '!=':return ComparisonOperator.neq
            case _: raise NotImplementedError

class ChainOperator(Enum):
    and_ = 'and'
    or_ = 'or'

class Condition(Qualifiable):
    attibuteParser = AttributeParser()
    def __init__(self, operand1:str, operator: ComparisonOperator|str, operand2:str) -> None:
        '''
        :param operand1: left operand
        :param operator: ComparisonOperator
        :param operand2: right operand
        ComparisonOperator is an enum with values like eq, ne, gt, lt, gte, lte
        Currently assumes each condition is a binary condition.
        '''
        if len(operand1) == 0 or len(operand2) == 0:
            raise ValueError("Operands cannot be empty")
        self.operand1 = operand1
        self.operand2 = operand2
        self.operator = ComparisonOperator.from_str(operator) if isinstance(operator, str) else operator
        self._identified_columns = self.attibuteParser.extract_columns([operand1, operand2])
        self._qual_operand1 = operand1
        self._qual_operand2 = operand2
    def __str__(self) -> str:
        return f"({self._qual_operand1} {self.operator.value} {self._qual_operand2})"
    @property
    def identified_columns(self) -> list[str]:
        return self._identified_columns
    def to_str(self) -> str:
        return self.__str__()
    def qualify(self, mapping: dict[str, Dataset]) -> None:
        qual_op1 = self.operand1
        qual_op2 = self.operand2
        
        for col in self._identified_columns:
            qualified_col = f"{mapping[col].name}.{col}" 
            qual_op1 = qual_op1.replace(col, qualified_col)
            qual_op2 = qual_op2.replace(col, qualified_col)
        self._qual_operand1 = qual_op1
        self._qual_operand2 = qual_op2
        pass



class ConditionNode(object):
    def __init__(self, condition: Condition):
        self.condition = condition
        self.nextNode:ConditionNode|None = None
        self.chainingOp:ChainOperator|None = None

class ChainedConditionsIterator:
    def __init__(self, head: ConditionNode):
        self.current = head
    def __iter__(self):
        return self
    def __next__(self):
        if not self.current:
            raise StopIteration
        else:
            currCondition = self.current.condition
            self.current = self.current.nextNode
            return currCondition
class ChainedConditions(object):
    def __init__(self, condition: Condition):
        self.head = ConditionNode(condition)
        self.tail = self.head
        self.numNodes = 1
    def __iter__(self):
        return ChainedConditionsIterator(self.head)

    def add(self, condition: Condition, chainingOp: ChainOperator):
        lastNode = self.tail
        lastNode.chainingOp = chainingOp
        lastNode.nextNode = ConditionNode(condition)
        self.tail = lastNode.nextNode
        self.numNodes += 1
    def as_str(self)->str:
        node = self.head
        printval = ""
        while node is not None:
            printval = f"{printval}{node.condition.to_str()}{'' if node.chainingOp is None else node.chainingOp.value}"
            node = node.nextNode
        return printval

