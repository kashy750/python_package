# General Utilities

Includes:
1. Logger Module
2. Redis Client
3. RabbitMQ Client
4. MinIo Client



CLASSES:
    
    class FileStorage(builtins.object)
     |  FileStorage(url, user, pwd)
     |  
     |  Used for interacting wiht Redis.
     |  Args:
     |      host (str): url for connection
     |      user (str): user name
     |      pwd (str): password
     |  Returns:
     |      None
     |  Methods:
     |      get_data() : for fetching data corresponding to a redis-key
     |  
     |  Methods defined here:
     |  
     |  __init__(self, url, user, pwd)
     |      Initialize self.  See help(type(self)) for accurate signature.
     |  
     |  get_data(self, bucket_name, file_name, data_type='csv')
     |      Used for fetching a file from MinIO from a specific bucket.
     |      Args:
     |          bucket_name (str): MinIO bucket name
     |          file_name (str): unique file name 
     |          data_type (str)[default:'csv']: type of data to read
     |      Returns:
     |          Data (df/dict)
     |  
     |  set_connection(self, url, user, pwd)
     |  
     |  ----------------------------------------------------------------------
     |  Data descriptors defined here:
     |  
     |  __dict__
     |      dictionary for instance variables (if defined)
     |  
     |  __weakref__
     |      list of weak references to the object (if defined)
    
    class REDIS(builtins.object)
     |  REDIS(host, port)
     |  
     |  Used for interacting wiht Redis.
     |  Args:
     |      host (str): host for connection
     |      port (int): port as int
     |  Returns:
     |      None
     |  Methods:
     |      get_data() : for fetching data corresponding to a redis-key
     |      set_data() : for insert data corresponding to a redis-key
     |  
     |  Methods defined here:
     |  
     |  __init__(self, host, port)
     |      Initialize self.  See help(type(self)) for accurate signature.
     |  
     |  get_data(self, redis_key, data_type='dict')
     |      Used for fetching data from Redis corresponding to a specific key.
     |      Args:
     |          redis_key (str): uniquu redis-key
     |          data_type (str)[default:'dict']: type of data stored with the redis-key (options:dict, df) 
     |      Returns:
     |          Data (df/dict)
     |  
     |  set_connection(self, host, port)
     |  
     |  set_data(self, redis_key, data, data_type='dict')
     |      Used for inserting data to Redis corresponding to a specific key.
     |      Args:
     |          redis_key (str): uniquu redis-key
     |          redis_key (str): uniquu redis-key
     |          data_type (str)[default:'dict']: type of data stored with the redis-key (options:dict, df) 
     |      Returns:
     |          Data (df/dict)
     |  
     |  ----------------------------------------------------------------------
     |  Data descriptors defined here:
     |  
     |  __dict__
     |      dictionary for instance variables (if defined)
     |  
     |  __weakref__
     |      list of weak references to the object (if defined)
    
    class RMQ(builtins.object)
     |  RMQ(host, port, user, pwd)
     |  
     |  Used for interacting wiht RabbitMQ.
     |  Args:
     |      host (str): host for connection
     |      port (int): port as int
     |      user (str): user_name
     |      pwd (str): password
     |  Returns:
     |      None
     |  Methods:
     |      listen() : for consuming messages
     |      publish() : for sending mesages
     |  
     |  Methods defined here:
     |  
     |  __init__(self, host, port, user, pwd)
     |      Initialize self.  See help(type(self)) for accurate signature.
     |  
     |  callback(self, ch, method, properties, body)
     |  
     |  listen(self, consume_queue, callback_func, publish_fl=False, publish_queue='')
     |      Used for listening to messages (continuous).
     |      Args:
     |          consume_queue (str): queue_name to listen
     |          callback_func (function): pass a funtion which takes a str as input(the message recieved)
     |          publish_fl (bool)[default:True]: 'True', if some message to publish after callback_func()
     |          publish_queue (str)[default:""]: queue_name to publish
     |      Returns:
     |          None [continuously waits for messages, never terminate]
     |  
     |  publish(self, msg_dict, publish_queue)
     |      Used for publishing messages.
     |      Args:
     |          msg_dict (dict): message to publish (dict)
     |          publish_queue (str): queue_name to publish
     |      Returns:
     |          None
     |  
     |  set_connection(self, host, port, user, pwd)
     |  
     |  ----------------------------------------------------------------------
     |  Data descriptors defined here:
     |  
     |  __dict__
     |      dictionary for instance variables (if defined)
     |  
     |  __weakref__
     |      list of weak references to the object (if defined)

	FUNCTIONS
	    logger()
	        Used for logging in cmd line.
	        Args:
	            None
	        Returns:
            None