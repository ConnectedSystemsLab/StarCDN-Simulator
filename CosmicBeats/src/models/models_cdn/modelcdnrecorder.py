from typing import List, Tuple

from src.models.imodel import IModel, EModelTag
from src.nodes.inode import INode
from src.simlogging.ilogger import ILogger
from src.simlogging.ilogger import ILogger, ELogType

from src.models.models_cdn.eviction_strategy.lrueviction import lruStrategy

class ModelCDNRecorder(IModel):
   
    __modeltag = EModelTag.VIEWOFNODE
    __ownernode: INode
    __supportednodeclasses = []  
    __dependencies = []
    __logger: ILogger
    
    @property
    def iName(self) -> str:
        """
        @type 
            str
        @desc
            A string representing the name of the model class. For example, ModelPower 
            Note that the name should exactly match to your class name. 
        """
        return self.__class__.__name__
    
    @property
    def modelTag(self) -> EModelTag:
        """
        @type
            EModelTag
        @desc
            The model tag for the implemented model
        """
        return self.__modeltag

    @property
    def ownerNode(self):
        """
        @type
            INode
        @desc
            Instance of the owner node that incorporates this model instance.
            The subclass (implementing a model) should keep a private variable holding the owner node instance. 
            This method can return that variable.
        """
        return self.__ownernode
    
    @property
    def supportedNodeClasses(self) -> 'list[str]':
        '''
        @type
            List of string
        @desc
            A model may not support all the node implementation. 
            supportedNodeClasses gives the list of names of the node implementation classes that it supports.
            For example, if a model supports only the SatBasic and SatAdvanced, the list should be ['SatBasic', 'SatAdvanced']
            If the model supports all the node implementations, just keep the list EMPTY.
        '''
        return self.__supportednodeclasses
    
    @property
    def dependencyModelClasses(self) -> 'list[list[str]]':
        '''
        @type
            Nested list of string
        @desc
            dependencyModelClasses gives the nested list of name of the model implementations that this model has dependency on.
            For example, if a model has dependency on the ModelPower and ModelOrbitalBasic, the list should be [['ModelPower'], ['ModelOrbitalBasic']].
            Now, if the model can work with EITHER of the ModelOrbitalBasic OR ModelOrbitalAdvanced, the these two should come under one sublist looking like [['ModelPower'], ['ModelOrbitalBasic', 'ModelOrbitalAdvanced']]. 
            So each exclusively dependent model should be in a separate sublist and all the models that can work with either of the dependent models should be in the same sublist.
            If your model does not have any dependency, just keep the list EMPTY. 
        '''
        return self.__dependencies
    
    def __str__(self) -> str:
        return "".join(["Model name: ", self.iName, ", " , "Model tag: ", self.__modeltag.__str__()])

    def call_APIs(
            self,   
            _apiName: str, 
            **_kwargs):
        '''
        This method acts as an API interface of the model. 
        An API offered by the model can be invoked through this method.
        @param[in] _apiName
            Name of the API. Each model should have a list of the API names.
        @param[in]  _kwargs
            Keyworded arguments that are passed to the corresponding API handler
        @return
            The API return
        '''
        _ret = None

        _ret = self.__apiHandlerDictionary[_apiName](self, **_kwargs) 
        return _ret
    
    def __init__(
        self, 
        _ownernodeins: INode, 
        _loggerins: ILogger
    ) -> None:
        '''
        @desc
            Constructor of the class
        @param[in]  _ownernodeins
            Instance of the owner node that incorporates this model instance
        @param[in]  _loggerins
            Logger instance
        @param[in]  _minElevation
            Minimum elevation angle of view in degrees
        '''
        assert _ownernodeins is not None
        assert _loggerins is not None

        self.__logger = _loggerins
        self.__ownernode = _ownernodeins

        self.__request_queue = [] 
                            
    def Execute(self) -> None:
        pass
        

    def __handle_requests(self, **kwargs) -> list:
        requests: list = kwargs['requests'] # This is a list of [time id, size, user] sublist
        for request in requests:
            self.__request_queue.append(list(request))

    def __no_op(self, **kwargs):
        pass

    # Process all requests in this epoch 
    def __post_epoch_hook(self, **kwargs):
        if len(self.__request_queue) == 0:
            self.__logger.write_Log("No traffic received in this epoch", ELogType.LOGDEBUG, self.__ownernode.timestamp)
        else:
            sorted(self.__request_queue, key = lambda x:x[0]) # Sort based on time, no tie breaking
            self.__logger.write_Log(f"[Requests]:{list(self.__request_queue)}", ELogType.LOGDEBUG, self.__ownernode.timestamp)
            self.__request_queue.clear()
    
    __apiHandlerDictionary = {
        "handle_requests": __handle_requests,
        "post_epoch_hook": __post_epoch_hook
    }

    __cacheEvictionStrategyDictionary = {
        'LRU': lruStrategy
    }

    __activeSchedulingStrategyDictionary = {
        "no_op": __no_op
    }

def init_ModelCDNProvider(
                    _ownernodeins: INode, 
                    _loggerins: ILogger, 
                    _modelArgs) -> IModel:
    '''
    @desc
        This method initializes an instance of ModelFovTimeBased class
    @param[in]  _ownernodeins
        Instance of the owner node that incorporates this model instance
    @param[in]  _loggerins
        Logger instance
    @param[in]  _modelArgs
        It's a converted JSON object containing the model related info. 
        @key min_elevation
            Minimum elevation angle of view in degrees
    @return
        Instance of the model class
    '''
    # check the arguments
    assert _ownernodeins is not None
    assert _loggerins is not None
    
    return ModelCDNProvider(_ownernodeins, 
                            _loggerins)
