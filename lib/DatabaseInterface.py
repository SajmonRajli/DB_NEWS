from abc import ABC, abstractmethod

#абстрактный класс для реализации интерфейса классов связующих функции 
#методы этого класса должны быть реализованы в каждом классе наследнике
#при реализации интерфейса нужно определить все методы какие будут использоваться в классах наследниках

class DatabaseInterface(ABC):
    
    @abstractmethod
    def create(self, table: str, values: dict):
        #create request to create table
        pass

    @abstractmethod
    def set(self, table: str, values: dict):
        #create request to insert data
        pass

    @abstractmethod
    def get(self, table: str, where: str = None):
        #create request to select data
        pass

    @abstractmethod
    def update(self, table: str, values: dict, where: str = None):
        #create request to update data
        pass

    @abstractmethod
    def delete(self, table: str, where: str):
        #create request to delete data
        pass
