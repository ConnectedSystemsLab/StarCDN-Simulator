from src.models.imodel import IModel, EModelTag
from src.nodes.inode import INode, ENodeType
from src.nodes.itopology import ITopology
from src.simlogging.ilogger import ILogger
from src.sim.imanager import EManagerReqType
from src.simlogging.ilogger import ILogger, ELogType

from src.utils import File

import numpy as np
import hashlib

class ModelCDNUser(IModel):
   
    __modeltag = EModelTag.CDN
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

        try:
            _ret = self.__apiHandlerDictionary[_apiName](self, **_kwargs)
        except Exception as e:
            print(f"[ModelFoVTimeBased]: An unhandled API request has been received by {self.__ownernode.nodeID}: ", e)
        
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

        self.__ownernode = _ownernodeins
        self.__logger = _loggerins
        self.__requests: list[File] = []
        self.__sat_to_schedule = 5 
        self.__myTopology: ITopology = None
    
    def __set_my_topology(self):
        _topologyID = self.__ownernode.topologyID
        _topologies = self.__ownernode.managerInstance.req_Manager(EManagerReqType.GET_TOPOLOGIES)
            
        for _topology in _topologies:
            if _topology.id == _topologyID:
                self.__myTopology = _topology
                break 

    def Execute(self) -> None:
        self.__set_my_topology()
        if len(self.__requests) == 0:
            self.__logger.write_Log(f"Not request scheduled for this epcoh", ELogType.LOGINFO, self.__ownernode.timestamp)
            return
        # Generate some accesses
        total_requests = self.__requests
        # Schedule one satellite
        targetSatellites: list = self.__ownernode.has_ModelWithName('ModelFovTimeBased').call_APIs('get_View', 
                                                                                                   _targetNodeTypes=[ENodeType.SAT], _myTime=self.__ownernode.timestamp)
        if targetSatellites is None or len(targetSatellites) == 0:
            self.__logger.write_Log(f"[Warning]: Out of service", ELogType.LOGINFO, self.__ownernode.timestamp)
            self.__requests.clear()
            return

        # Send requests to the scheduled satellites
        sat_to_schedule = min(min(len(targetSatellites), self.__sat_to_schedule), len(total_requests))
        requestsPerSat = [[] for _ in range(sat_to_schedule)]
        # for req in total_requests:
            # requestsPerSat[int(hashlib.md5(req.id.encode()).hexdigest(), 16) % sat_to_schedule].append(req)
        requestsPerSat = np.array_split(total_requests, sat_to_schedule)
        for i in range(sat_to_schedule):
            targetSatellite = self.__myTopology.get_Node(targetSatellites[i])
            requests = requestsPerSat[i]
            cdn_cache_hit_results = targetSatellite.has_ModelWithName('ModelCDNProvider').call_APIs('handle_requests', requests=list(requests), user_id = self.__ownernode.nodeID) 
            self.__logger.write_Log(f"[Request Result]:{targetSatellite.nodeID},{cdn_cache_hit_results}", 
                                    ELogType.LOGINFO, self.__ownernode.timestamp)
        self.__requests.clear()

    def __add_request(self, **kwargs):
        self.__requests.append(kwargs['request'])

    __apiHandlerDictionary = {
        'add_request': __add_request
    }

    
        
def init_ModelCDNUser(
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

    return ModelCDNUser(_ownernodeins, 
                        _loggerins, 
                        )
