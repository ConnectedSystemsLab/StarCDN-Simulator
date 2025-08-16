import threading
import queue
import numpy as np
from src.nodes.inode import INode, ENodeType

from collections import OrderedDict, deque

from src.models.imodel import IModel, EModelTag
from src.nodes.inode import INode
from src.nodes.itopology import ITopology
from src.simlogging.ilogger import ILogger
from src.sim.imanager import EManagerReqType
from src.simlogging.ilogger import ILogger, ELogType
from src.nodes.topology import Topology

from src.utils import Location
from src.utils import File

from src.models.models_cdn.cache.lru import LRU_Cache

import json 
import hashlib

NUM_COLOR = 25 

class ModelCDNProvider(IModel):
   
    __modeltag = EModelTag.VIEWOFNODE
    __ownernode: INode
    __supportednodeclasses = []  
    __dependencies = []
    __logger: ILogger
    
    __global_cache = {}
    cafe_push_back = True 

    @property
    def cache(self):
        return self.__cache

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

        # try:
        #     _ret = self.__apiHandlerDictionary[_apiName](self, **_kwargs)
        # except Exception as e:
        #     print(f"[ModelCDNProvider]: An unhandled API request has been received by {self.__ownernode.nodeID}: ", e)
        _ret = self.__apiHandlerDictionary[_apiName](self, **_kwargs) 
        return _ret
    

    def __init__(
        self, 
        _ownernodeins: INode, 
        _loggerins: ILogger,
        _cacheCapacity: int,
        _topologyFile: str,
        _handleRequestsStrategy: str,
        _activeSchedulingStrategy: str,
        _neighbors: list,
        _useGS: bool,
        _prefetch_byte: float,
        _allow_uplink: bool,
        _prefetch_strategy: str
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

        self.__cache = LRU_Cache(_cacheCapacity)
        self.__metadata_cache = {}
        self.__cacheSize = 0 
        self.__cacheCapacity= _cacheCapacity 
        self.__handleRequestsStrategy: callable = self.__handleRequestsStrategyDictionary[_handleRequestsStrategy]
        self.__activeSchedulingStrategy: callable = self.__activeSchedulingStrategyDictionary[_activeSchedulingStrategy]
        self.__neighbors = _neighbors

        
        self.__useGS: bool = _useGS

        self.__lock = threading.Lock()
        self.__myTopology:ITopology = None

        self.__ingress_traffic = [0, 0, 0, 0, 0, 0]
        self.__egress_traffic = [0, 0, 0, 0, 0, 0]
        self.__seen = set()

        self.__time = 0
        self.__hit_or_admit = set()
        self.__uplink = 0
        self.__downlink = 0
        self.__closest_gs = None             
        self.__prefetch_byte = _prefetch_byte
        self.__allow_uplink = _allow_uplink
        self.__byte_hit = 0
        self.__isl = [0, 0, 0, 0]
        self.__prefetch_strategy = _prefetch_strategy
        # with open("../isl/sat_color_2_hops.json", "r") as f:
        # with open("../isl/sat_color_3_hops.json", "r") as f:
        # with open("../isl/sat_color_16.json", "r") as f:
        with open(_topologyFile, "r") as f:
            data = json.load(f)
            self.hash_number = data[str(self.__ownernode.nodeID)]
        self.__hash_buckets = None 


    def Execute(self) -> None:
        # Run active scheduling policies
        self.__activeSchedulingStrategy(self)
        # latency = [] 
        # self.__set_my_topology()
        # for neighbor in self.__neighbors:
        #     neighbor_node = self.__myTopology.get_Node(neighbor)
        #     latency.append(neighbor_node.get_Position(self.__ownernode.timestamp).get_distance(self.__ownernode.get_Position(self.__ownernode.timestamp)) / 3e8)
        # print(latency)
        if self.ownerNode.nodeID in [1008, 1693, 2412]:
            self.__logger.write_Log(f'[Location]: {self.ownerNode.get_Position(self.ownerNode.timestamp).to_lat_long()}', ELogType.LOGALL, self.__ownernode.timestamp, self.iName) 
        

    def __handle_requests(self, **kwargs) -> list:
        return self.__handleRequestsStrategy(self, **kwargs)

    def __check_one_hop(self, **kwargs):
        """
        @desc
            This method handles a list request by chekcing local cache and
            remote cache maximum 1 hop away
        """
        self.__lock.acquire()
        if self.__myTopology is None:
            self.__set_my_topology()

        requests :list[File] = kwargs['requests']
        hits = []
        for request in requests:

            if request.id in self.__cache:
                # This is a cache hit
                self.__cache.pop(request.id)
                self.__cache[request.id] = request.size
                hits.append("Local")

            else:
                # We check if we can find a remote replicas
                remote_replicas_node: INode = None
                i = 3
                while i >= 0:
                    neighbor_node = self.__myTopology.get_Node(self.__neighbors[i])
                    if neighbor_node.has_ModelWithName('ModelCDNProvider').call_APIs('check_in_cache', request_id=request.id):
                        remote_replicas_node = neighbor_node
                        break
                    i -= 1
                
                if remote_replicas_node is None:
                    # Remote miss fetch from ground station
                    hits.append('Miss')
                    self.__ingress_traffic[1] += request.size 
                    if request.id in self.__seen:
                        self.__admit(request)
                    else:
                        self.__seen.add(request.id)
                    # self.__push_to_left_right(request)
                else:
                    hits.append('Remote')
                    # print(f"Dist: {remote_replicas_node.get_Position(self.__ownernode.timestamp).get_distance(self.__ownernode.get_Position(self.__ownernode.timestamp))}")
                    self.__ingress_traffic[i + 2] += request.size 
                    # self.__push_to_left_right(request)
                    if request.id in self.__seen:
                        self.__admit(request)
                    else:
                        self.__seen.add(request.id)
                
            # Add downlink for each request
            self.__egress_traffic[0] += request.size 
        self.__lock.release()
        return hits 
    
    def __check_one_hop_no_replicas(self, **kwargs):
        """
        @desc
            This method handles a list request by chekcing local cache and
            remote cache maximum 1 hop away
        """
        self.__lock.acquire()
        if self.__myTopology is None:
            self.__set_my_topology()

        requests :list[File] = kwargs['requests']
        hits = []
        for request in requests:
            if request.id in self.__cache:
                # This is a cache hit
                self.__cache.pop(request.id)
                self.__cache[request.id] = request.size
                hits.append("Local")

            else:
                # We check if we can find a remote replicas
                remote_replicas_node: INode = None
                i = 3
                while i >= 0:
                    neighbor_node = self.__myTopology.get_Node(self.__neighbors[i])
                    if neighbor_node.has_ModelWithName('ModelCDNProvider').call_APIs('check_in_cache', request_id=request.id):
                        remote_replicas_node = neighbor_node
                        break
                    i -= 1
                
                if remote_replicas_node is None:
                    # Remote miss fetch from ground station
                    hits.append('Miss')
                    self.__ingress_traffic[1] += request.size 
                    if request.id in self.__seen:
                        self.__admit(request)
                    else:
                        self.__seen.add(request.id)
                    # self.__push_to_left_right(request)
                else:
                    hits.append('Remote')
                    # print(f"Dist: {remote_replicas_node.get_Position(self.__ownernode.timestamp).get_distance(self.__ownernode.get_Position(self.__ownernode.timestamp))}")
                    self.__ingress_traffic[i + 2] += request.size 
                    # self.__push_to_left_right(request)
                
            # Add downlink for each request
            self.__egress_traffic[0] += request.size 
        self.__lock.release()
        return hits 
    
    def __check_one_hop_no_bloom(self, **kwargs):
        """
        @desc
            This method handles a list request by chekcing local cache and
            remote cache maximum 1 hop away
        """
        self.__lock.acquire()
        if self.__myTopology is None:
            self.__set_my_topology()

        requests :list[File] = kwargs['requests']
        hits = []
        for request in requests:
            if request.id in self.__cache:
                # This is a cache hit
                self.__cache.pop(request.id)
                self.__cache[request.id] = request.size
                hits.append("Local")

            else:
                # We check if we can find a remote replicas
                remote_replicas_node: INode = None
                i = 3
                while i >= 0:
                    neighbor_node = self.__myTopology.get_Node(self.__neighbors[i])
                    if neighbor_node.has_ModelWithName('ModelCDNProvider').call_APIs('check_in_cache', request_id=request.id):
                        remote_replicas_node = neighbor_node
                        break
                    i -= 1
                
                if remote_replicas_node is None:
                    # Remote miss fetch from ground station
                    hits.append('Miss')
                    self.__ingress_traffic[1] += request.size 
                    self.__admit(request)
                    # self.__push_to_left_right(request)
                else:
                    hits.append(f'Remote{i}')
                    # print(f"Dist: {remote_replicas_node.get_Position(self.__ownernode.timestamp).get_distance(self.__ownernode.get_Position(self.__ownernode.timestamp))}")
                    self.__ingress_traffic[i + 2] += request.size 
                    # self.__push_to_left_right(request)
                    self.__admit(request) 
                
            # Add downlink for each request
            self.__egress_traffic[0] += request.size 
        self.__lock.release()
        return hits 
        
    def __check_local_only(self, **kwargs):
        """
        @desc
            This method handles requests by checking only local caches
        """
        if self.__myTopology is None:
            self.__set_my_topology()

        requests :list[File] = kwargs['requests']
        hits = []
        
        for request in requests:
            if request.id in self.__cache:
                # This is a cache hit
                self.__cache.pop(request.id)
                self.__cache[request.id] = request.size
                hits.append("1")

            else:
                # Remote miss fetch from ground station
                hits.append('0')

                if request.id in self.__seen:
                    self.__admit(request)
                else:
                    self.__seen.add(request.id)
            # Add downlink for each request
        return hits 

    def __check_local_no_bloom(self, **kwargs):
        self.__lock.acquire()
        if self.__myTopology is None:
            self.__set_my_topology()

        requests :list[File] = kwargs['requests']
        hits = []
        
        for request in requests:
            if request.id in self.__cache:
                # This is a cache hit
                self.__cache.pop(request.id)
                self.__cache[request.id] = request.size
                hits.append("Local")

            else:
                # Remote miss fetch from ground station
                hits.append('Miss')
                self.__ingress_traffic[1] += request.size

                self.__admit(request)

                    
            # Add downlink for each request
            self.__egress_traffic[0] += request.size
        self.__lock.release()
        return hits  
    
    def __check_with_erasure_no_remote(self, **kwargs):
        if self.__myTopology is None:
            self.__set_my_topology()
        requests :list[File] = kwargs['requests']
        hits = [] 
       

        for request in requests:

            self.__metadata_cache.setdefault(request.id, [])
            shards = self.__metadata_cache[request.id]
            if len(shards) > 0:
                # Try to reconstruct locally first
                if self.__check_can_reconstruct(request_id = request.id):
                    self.__redistribute(request = request, suffix=0)
                    hits.append('Local')
                else:
                    remote_replicas_node = None
                    i = 0
                    while i < 4:
                        neighbor_node = self.__myTopology.get_Node(self.__neighbors[i])
                        if neighbor_node.has_ModelWithName('ModelCDNProvider').call_APIs('check_can_reconstruct', request_id=request.id):
                            remote_replicas_node = neighbor_node
                            break
                        i += 1
                    if remote_replicas_node is None:
                        # Can't construct at all
                        hits.append('Partial')
                        self.__redistribute(request = request, suffix=0)
                    else:
                        # Reconstruct at another source
                        hits.append("Parity")
                        neighbor_node.has_ModelWithName('ModelCDNProvider').call_APIs('redistribute', request=request, suffix = 0)

                
            else:
                # For now consider it as miss
                self.__redistribute(request = request, suffix = 0)
                hits.append("Miss")
        return hits


    
    def __check_can_reconstruct(self, **kwargs):
        if self.__myTopology is None:
            self.__set_my_topology()
        i = 0
        request_id = kwargs['request_id']
        seen = set()
        self.__metadata_cache.setdefault(request_id, [])
        shards = self.__metadata_cache[request_id]
        for shard in shards:
            seen.add(shard)
        while i < 4:
            neighbor_node = self.__myTopology.get_Node(self.__neighbors[i])
            shards = neighbor_node.has_ModelWithName('ModelCDNProvider').call_APIs('get_prefix_in_cache', request_id=request_id)
            for shard in shards:
                seen.add(shard)
            i += 1 
        if len(seen) >= 3:
            return True
        return False
    
    def __redistribute(self, **kwargs):
        request: File = kwargs['request']
        suffix = kwargs['suffix']
        # print(f"Redistribution, {request.id}, {request.size}, {suffix}")
        file_size = (request.size // 4) + 1
        if request.id in self.__cache:
            size = self.__cache.pop(request.id)
            # print('exist pop')
            self.__cacheSize -= size 
            self.__metadata_cache[request.id] = []
        if self.__cacheCapacity < file_size:
            return 
        # print(f'{self.ownerNode.nodeID}, before {self.__cacheSize}, {file_size}, {self.__cacheCapacity}, {len(self.__cache)}')
        while self.__cacheSize + file_size > self.__cacheCapacity:
            id, size = self.__cache.popitem(last=False)
            self.__cacheSize -= size 
            self.__metadata_cache[id] = []
            # print(f'{id},{size}')
            # print(f'{self.__cacheSize}, {file_size}, {self.__cacheCapacity}, {len(self.__cache)}')
            # ModelCDNProvider.__global_cache[id] -= 1 
        self.__cache[request.id] = file_size
        self.__metadata_cache[request.id] = [suffix]
        self.__cacheSize += file_size
        # print(f'after {self.__cacheSize}, {file_size}, {self.__cacheCapacity}, {len(self.__cache)}')

        if suffix == 0:
            for i in range(4):
                neighbor_node = self.__myTopology.get_Node(self.__neighbors[i])
                neighbor_node.has_ModelWithName('ModelCDNProvider').call_APIs('redistribute', request=request, suffix = i + 1) 
    
    def __check_lru_partition_cache(self, **kwargs):
        self.__lock.acquire()
        if self.__myTopology is None:
            self.__set_my_topology()

        requests :list[File] = kwargs['requests']
        hits = []
        
        for i in range(len(requests)):
            request = requests[i]
            self.__time = max(self.__time, request.time)
            if self.__partition_cache.contains_local(request.id):
                self.__partition_cache.update_local(request.id, request.time)
                self.__hit_or_admit.add(request.id)
                hits.append("Local")
            elif self.__partition_cache.contains_prep(request.id):
                self.__partition_cache.handle_prep_hit(request.id, request.time)
                self.__hit_or_admit.add(request.id)
                hits.append("Local")
            else:
                # Remote miss fetch from ground station
                hits.append('Miss')
                if self.__partition_cache.admit(request.id, request.size, request.time):
                    self.__hit_or_admit.add(request.id)
                self.__ingress_traffic[1] += request.size 

            # Add downlink for each request
            self.__egress_traffic[0] += request.size 
        self.__lock.release()
        return hits 
    
    def __check_cafe_cache(self, **kwargs):
        self.__lock.acquire()
        if self.__myTopology is None:
            self.__set_my_topology()

        requests :list[File] = kwargs['requests']
        hits = []
        
        for i in range(len(requests)):
            request = requests[i]
            self.__time = max(self.__time, request.time)
            if self.__cafe_cache.contains(request.id):
                # This is a cache hit
                self.__cafe_cache.update(request.id, request.time)
                self.__hit_or_admit.add(request.id)
                hits.append("Local")

            else:
                # Remote miss fetch from ground station
                hits.append('Miss')
                if self.__cafe_cache.admit(request.id, request.size, request.time):
                    self.__hit_or_admit.add(request.id)
                self.__ingress_traffic[1] += request.size 

            # Add downlink for each request
            self.__egress_traffic[0] += request.size 
        self.__lock.release()
        return hits 
    
    def __check_parition_cache(self, **kwargs):
        self.__lock.acquire()
        if self.__myTopology is None:
            self.__set_my_topology()

        requests :list[File] = kwargs['requests']
        hits = []
        
        for i in range(len(requests)):
            request = requests[i]
            self.__time = max(self.__time, request.time)
            if self.__partition_cache.contains_local(request.id):
                self.__partition_cache.update_local(request.id, request.time)
                self.__hit_or_admit.add(request.id)
                hits.append("Local")
            elif self.__partition_cache.contains_prep(request.id):
                self.__partition_cache.handle_prep_hit(request.id, request.time)
                self.__hit_or_admit.add(request.id)
                hits.append("Local")
            else:
                # Remote miss fetch from ground station
                hits.append('Miss')
                if self.__partition_cache.admit(request.id, request.size, request.time):
                    self.__hit_or_admit.add(request.id)
                self.__ingress_traffic[1] += request.size 

            # Add downlink for each request
            self.__egress_traffic[0] += request.size 
        self.__lock.release()
        return hits 
    
    def __check_collaborate_cafe_cache(self, **kwargs):
        self.__lock.acquire()
        if self.__myTopology is None:
            self.__set_my_topology()

        requests :list[File] = kwargs['requests']
        hits = []
        for request in requests:
            self.__time = max(self.__time, request.time)
            if self.__cafe_cache.contains(request.id):
                # This is a cache hit
                self.__cafe_cache.update(request.id, request.time)
                self.__hit_or_admit.add(request.id)
                hits.append("Local")

            else:
                # We check if we can find a remote replicas
                remote_replicas_node: INode = None
                i = 3
                while i >= 0:
                    neighbor_node = self.__myTopology.get_Node(self.__neighbors[i])
                    if neighbor_node.has_ModelWithName('ModelCDNProvider').call_APIs('check_in_cafe_cache', request_id=request.id):
                        remote_replicas_node = neighbor_node
                        break
                    i -= 1
                
                if remote_replicas_node is None:
                    # Remote miss fetch from ground station
                    hits.append('Miss')
                    self.__ingress_traffic[1] += request.size 
                    if self.__cafe_cache.admit(request.id, request.size, request.time):
                        self.__hit_or_admit.add(request.id)
                    # self.__push_to_left_right(request)
                else:
                    hits.append('Remote')
                    # print(f"Dist: {remote_replicas_node.get_Position(self.__ownernode.timestamp).get_distance(self.__ownernode.get_Position(self.__ownernode.timestamp))}")
                    self.__ingress_traffic[i + 2] += request.size 
                    # self.__push_to_left_right(request)
                    self.__cafe_cache.admit(request.id, request.size, request.time)
                    self.__hit_or_admit.add(request.id)
                
            # Add downlink for each request
            self.__egress_traffic[0] += request.size 
        self.__lock.release()
        return hits 
    
    def __admit(self, file: File):
        if self.__cacheCapacity < file.size:
            return
        if self.__cacheSize + file.size < self.__cacheCapacity:
            self.__cache[file.id] = file.size
            self.__cacheSize += file.size
            # Record to global cache for monitor purpose
            # if file.id not in ModelCDNProvider.__global_cache:
            #     ModelCDNProvider.__global_cache[file.id] = 0 
            # ModelCDNProvider.__global_cache[file.id] += 1 
        else:
            while self.__cacheSize + file.size > self.__cacheCapacity:
                id, size = self.__cache.popitem(last=False)
                self.__cacheSize -= size 
                # ModelCDNProvider.__global_cache[id] -= 1 
            self.__cache[file.id] = file.size 
            self.__cacheSize += file.size
            # if file.id not in ModelCDNProvider.__global_cache:
            #     ModelCDNProvider.__global_cache[file.id] = 0 
            # ModelCDNProvider.__global_cache[file.id] += 1 

    def __check_in_cache(self, **kwargs):
        """
        @desc
            API for checking if a request is in current node's cache
        """
        return  kwargs['request_id'] in self.__cache
    
    def __check_prefix_in_cache(self, **kwargs):
        """
        @desc
            API for checking if a request id as prefix is in current node's cache
        """
        self.__metadata_cache.setdefault(kwargs["request_id"], [])
        return len(self.__metadata_cache[kwargs["request_id"]]) > 0

    def __get_prefix_in_cache(self, **kwargs) -> list[str]:
        self.__metadata_cache.setdefault(kwargs["request_id"], [])
        return self.__metadata_cache[kwargs["request_id"]]
    
    def __no_op(self, **kwargs):
        pass

    def __search_neighbors(self, target, hops = 1):
        if hops == 0:
            return False, idx 
        q = queue.Queue()
        seen = set()
        seen.add(self.__ownernode.nodeID)
        for idx, neigh in enumerate(self.__neighbors):
            if int(neigh) == -1:
                continue
            q.put((neigh, 1, idx))
            seen.add(int(neigh))
        while not q.empty():
            sat_id, dist, idx = q.get()
            node = self.__myTopology.get_Node(int(sat_id))
            if node == None:
                continue
            if node.has_ModelWithName('ModelCDNProvider').call_APIs('in_cache', id = target):
                # print(sat_id, self.__ownernode.nodeID, dist, idx)
                return True, idx
            if dist < hops:
                for neigh in node.has_ModelWithName('ModelCDNProvider').call_APIs('get_neighbors'):
                    if int(neigh) not in seen and int(neigh) != -1:
                        seen.add(int(neigh))
                        q.put((neigh, dist + 1, idx))
        return False, idx 

    def __get_neighbors(self):
        return self.__neighbors

    def __post_epoch_hook(self, **kwargs):
        if self.__myTopology is None:
            self.__set_my_topology() 
        # self.__hash_bfs()
        self.__logger.write_Log(f'uplink:{self.__uplink}, downlink:{self.__downlink}, byte_hit:{self.__byte_hit}', ELogType.LOGALL, self.__ownernode.timestamp, self.iName) 

        self.__ingress_traffic = [0, 0, 0, 0, 0, 0] 
        self.__egress_traffic = [0, 0, 0, 0, 0, 0] 
        self.__isl = [0] * 4
        self.__uplink = 0
        self.__downlink = 0
        self.__byte_hit = 0

        self.__closest_gs = None
        self.__hit_or_admit.clear() 

    def __prev_epoch_hook(self, **kwargs):
        if self.__myTopology is None:
            self.__set_my_topology()
        if self.__useGS:
            prefetch_byte = 0
            targetGS: list = self.__ownernode.has_ModelWithName('ModelFovTimeBased').call_APIs('get_View', 
                                                                                                _targetNodeTypes=[ENodeType.GS], _myTime=self.__ownernode.timestamp)
            if targetGS and len(targetGS) > 0:
                bytes_in_cache = 0
                gs_list = [self.__myTopology.get_Node(i) for i in targetGS]
                dist_list = np.array([[x.lat, x.lon] for x in gs_list])
                my_lat, my_lon = self.__ownernode.get_Position(self.__ownernode.timestamp).to_lat_long()[:2]
                distances = np.sqrt((dist_list[:, 0] - my_lat)**2 + (dist_list[:, 1] - my_lon)**2)
                connected_gs: INode = gs_list[np.argmin(distances)] 

                for id, size, _ in connected_gs.has_ModelWithName('ModelCDNGs').call_APIs(self.__prefetch_strategy):
                    already_in_cache = id in self.__cache
                    fetch_from_neigh = False
                    if not already_in_cache:
                        fetch_from_neigh, idx = self.__search_neighbors(id, 1) 
                        if fetch_from_neigh:
                            self.__isl[idx] += size
                    else:
                        bytes_in_cache += size
                    
                    if not already_in_cache and not fetch_from_neigh:
                        if not self.__allow_uplink:
                            # Give up this fetch
                            continue
                        else:
                            # Use uplink and admit the fetch
                            self.__uplink += size 

                    # Fetch the content if can be found in neighbor
                    self.__cache.admit(id, size, 0)
                    prefetch_byte += size

                    if prefetch_byte > self.__prefetch_byte:
                        break
                self.__closest_gs = connected_gs
                connected_gs.has_ModelWithName('ModelCDNGs').call_APIs('write_prefetch_stat', uplink=self.__uplink, isl=self.__isl, in_cache=bytes_in_cache)
                self.__logger.write_Log(f'[Prefetch stat]:[{self.__uplink}, {bytes_in_cache}, {self.__isl}]', ELogType.LOGALL, self.__ownernode.timestamp, self.iName) 
                
    def __in_cache(self, **kwargs):
        return kwargs['id'] in self.__cache
    
    def __record(self, **kwargs):
        requests :list[File] = kwargs['requests'] 
        traffic = []
        for req in requests:
            traffic.append([req.id, req.size])
        self.__logger.write_Log(f'[Requests Records]: {kwargs["user_id"]}, {kwargs["hops"]},{traffic}', ELogType.LOGALL, self.__ownernode.timestamp, self.iName) 

    def __hash_bfs(self):
        self.__set_my_topology()
        l = [(self.__ownernode.nodeID, 0, [0, 0])]
        seen = set()
        seen.add(int(self.__ownernode.nodeID))
        d = {}
        hops_d = {}
        while len(l) != 0:
            cur, dist, hops = l[0]
            l.pop(0)
            node = self.__myTopology.get_Node(int(cur))
            hash_number = node.has_ModelWithName('ModelCDNProvider').hash_number
            if hash_number not in d:
                d[hash_number] = int(cur)
                hops_d[hash_number] = hops.copy()
            for idx, neigh in enumerate(node.has_ModelWithName('ModelCDNProvider').call_APIs('get_neighbors')):
                if int(neigh) not in seen and dist <= 4 and int(neigh) != -1:
                    new_hop = hops.copy()
                    new_hop[idx // 2] += 1
                    l.append((int(neigh), dist + 1, new_hop))
                    seen.add(int(neigh))
        self.__hash_buckets = [-1 for _ in range(NUM_COLOR)] 
        self.__hash_hops = [-1 for _  in range(NUM_COLOR)]
        for i in range(len(self.__hash_buckets)):
            if i in d:
                self.__hash_buckets[i] = d[i] 
                self.__hash_hops[i] = hops_d[i]
        print(f"[Link]: [{self.ownerNode.nodeID},{self.__hash_buckets}]")






    def __hash_check(self, **kwargs):
        requests :list[File] = kwargs['requests'] 
        # self.__ownernode.has_ModelWithName('ModelCDNProvider').call_APIs('record', requests=requests, user_id=kwargs["user_id"], hops = [0, 0])
        # return 
        if self.__hash_buckets == None:
            self.__hash_bfs()
        distributed_requests = [[] for _ in range(NUM_COLOR)]
        for req in requests:
            hash_id = int(hashlib.md5(req.id.encode()).hexdigest(), 16) % NUM_COLOR
            hash_bucket_idx = hash_id
            for i in range(NUM_COLOR):
                if self.__hash_buckets[hash_bucket_idx] != -1:
                    break
                else:
                    hash_bucket_idx = (hash_bucket_idx + 1) % NUM_COLOR
            assert self.__hash_buckets[hash_bucket_idx] != -1
            distributed_requests[hash_bucket_idx].append(req)
        for i, reqs in enumerate(distributed_requests):
            if len(distributed_requests[i]) != 0:
                self.__myTopology.get_Node(int(self.__hash_buckets[i])).has_ModelWithName('ModelCDNProvider').call_APIs('record', requests=reqs, user_id=kwargs["user_id"], hops = self.__hash_hops[i])
                


            





    def __check_lru(self, **kwargs):
        requests :list[File] = kwargs['requests']
        hit = 0
        hit_byte = 0
        total_byte = 0
        for req in requests:
            if req.id in self.__cache:
                hit += 1
                hit_byte += req.size 
            else:
                self.__uplink += req.size 
            self.__cache.admit(req.id, req.size, 0)
            total_byte += req.size
            if self.__useGS and self.__closest_gs:
                self.__closest_gs.has_ModelWithName('ModelCDNGs').call_APIs('request_uplink', id = req.id, size = req.size)


        total = len(requests) 
        self.__logger.write_Log(f'[Requests]:[{hit/total, hit_byte/total_byte}]', ELogType.LOGALL, self.__ownernode.timestamp, self.iName) 
        self.__byte_hit += hit_byte
        self.__downlink += total_byte 
        return [hit/total, hit_byte/total_byte]
    
    def __check_lru_on_demand(self, **kwargs):
        requests :list[File] = kwargs['requests']
        hit = 0
        hit_byte = 0
        total_byte = 0
        if 'cold_set' in kwargs:
            cold_cache = kwargs['cold_set']
        cold_miss_byte = 0
        cold_miss_recover = 0
        for req in requests:
            if req.id in self.__cache:
                hit += 1
                hit_byte += req.size 
            else:
                if 'cold_set' in kwargs and req.id in cold_cache:
                    cold_miss_byte += req.size
                in_neighbor, _ = self.__search_neighbors(req.id, 1)
                if not in_neighbor:
                    self.__uplink += req.size 
                else:
                    if 'cold_set' in kwargs and req.id in cold_cache:
                        cold_miss_recover += req.size
                    hit += 1
                    hit_byte += req.size  
            self.__cache.admit(req.id, req.size, 0)
            total_byte += req.size
            if self.__useGS and self.__closest_gs:
                self.__closest_gs.has_ModelWithName('ModelCDNGs').call_APIs('request_uplink', id = req.id, size = req.size)


        total = len(requests) 
        self.__logger.write_Log(f'[Requests]:[{hit}, {total}, {hit_byte}, {total_byte}]', ELogType.LOGALL, self.__ownernode.timestamp, self.iName) 
        self.__byte_hit += hit_byte
        self.__downlink += total_byte 
        if 'cold_set' in kwargs:
            print(f'{cold_miss_byte}, {cold_miss_recover}')
        return [hit/total, hit_byte/total_byte]

    def __push_to_all(self, request: File):
        # We check if we can find a remote replicas
        for neighbor_id in self.__neighbors:
            neighbor_node = self.__myTopology.get_Node(neighbor_id) 
            neighbor_node.has_ModelWithName('ModelCDNProvider').call_APIs('proactive_cache_push', request=request)
    
    def __proactive_cache_receiver(self, **kwarg):
        # Don't record the traffic yet
        request: File = kwarg['request']
        if request.id in self.__cache:
            return
        self.__admit(request)

    
    def __strategy_on_local_hit(self, request):
        self.__push_to_all(request)

    def __strategy_on_remote_hit(self, request):
        self.__push_to_all(request)

    def __get_cache(self):
        return self.__cache
    
    def __get_cafe_cache(self):
        return self.__cafe_cache

    def __get_partition_cache(self):
        return self.__partition_cache

    def __check_in_cafe_cache(self, **kwargs):
        return self.__cafe_cache.contains(kwargs['request_id'])


    def __set_my_topology(self):
        _topologyID = self.__ownernode.topologyID
        _topologies = self.__ownernode.managerInstance.req_Manager(EManagerReqType.GET_TOPOLOGIES)
            
            
        for _topology in _topologies:
            if _topology.id == _topologyID:
                self.__myTopology = _topology
                break

    __apiHandlerDictionary = {
        "handle_requests": __handle_requests,
        "post_epoch_hook": __post_epoch_hook,
        "prev_epoch_hook": __prev_epoch_hook,
        "proactive_cache_push": __proactive_cache_receiver,
        "check_in_cache": __check_in_cache,
        "check_in_cafe_cache": __check_in_cafe_cache,
        "check_prefix_in_cache": __check_prefix_in_cache,
        "get_prefix_in_cache": __get_prefix_in_cache,
        "redistribute": __redistribute,
        "check_can_reconstruct": __check_can_reconstruct,
        "get_cache": __get_cache,
        "get_cafe_cache": __get_cafe_cache,
        "get_partition_cache": __get_partition_cache,
        "in_cache": __in_cache,
        "get_neighbors": __get_neighbors,
        "record": __record
    }

    __handleRequestsStrategyDictionary = {
        "check_one_hop": __check_one_hop,
        "check_one_hop_no_replicas": __check_one_hop_no_replicas,
        "check_local_only": __check_local_only,
        "check_local_no_bloom": __check_local_no_bloom,
        "check_one_hop_no_bloom": __check_one_hop_no_bloom,
        "check_with_erasure_no_remote": __check_with_erasure_no_remote,
        "check_cafe_cache": __check_cafe_cache,
        "check_collaborate_cafe_cache": __check_collaborate_cafe_cache,
        "check_partition_cache": __check_parition_cache,
        "check_lru_partition_cache": __check_lru_partition_cache,
        "check_lru": __check_lru,
        "check_lru_on_demand": __check_lru_on_demand,
        "record": __record,
        "hash_check": __hash_check
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
    
    if "cache_size" not in _modelArgs:
        raise Exception("[ModelCDNProvider Error]: The model arguments should contain the cache_size parameter.") 

    if "topology_file" not in _modelArgs:
        raise Exception("[ModelCDNProvider Error]: The model arguments should contain the cache_eviction_strategy parameter.") 

    if "handle_requests_strategy" not in _modelArgs:
        raise Exception("[ModelCDNProvider Error]: The model arguments should contain the handle_requests_strategy parameter.") 

    if "active_scheduling_strategy" not in _modelArgs:
        raise Exception("[ModelCDNProvider Error]: The model arguments should contain the active_scheduling_stratey parameter.") 

    if "neighbors" not in _modelArgs:
        raise Exception("[ModelCDNProvider Error]: The model arguments should contain the neighbors parameter.") 

    return ModelCDNProvider(_ownernodeins, 
                            _loggerins, 
                            _modelArgs.cache_size, 
                            _modelArgs.topology_file, 
                            _modelArgs.handle_requests_strategy, 
                            _modelArgs.active_scheduling_strategy,
                            _modelArgs.neighbors,
                            _modelArgs.useGS,
                            _modelArgs.prefetch_byte,
                            _modelArgs.allow_uplink,
                            _modelArgs.prefetch_strategy
                            )
