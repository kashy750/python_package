"""
A General Utilities Library that provides a Python interface to many technologies

Available Objects:
1. RMQ - RabbitMQ for queueing purpose
2. REDIS - Redis as a secondary storage
3. FileStorage - MinIO for storing and retrieving files
4. APIrequest - for API requests

Available Functions:
1. logger - Logging purpose
"""

# from gevent import monkey as curious_george
# curious_george.patch_all(thread=False, select=False)

import warnings
warnings.simplefilter(action='ignore')

import logging
from importlib import reload # https://stackoverflow.com/a/53553516
import pika
from minio import Minio
import redis
import pyarrow as pa
import grequests, requests
import functools
import threading
import uuid
import pandas as pd
import json
import io



def logger(level=logging.INFO, timeStamp_fl=True, processId_fl=False, extraLogs=""):
    """
    Used for logging in cmd line.
    Args:
        level (logging-level)[defauolt: logging.INFO]: level of logging required
        timeStamp_fl (bool)[default:True]: flag variable to mark timestamp in logs
        processId_fl (bool)[default:False]: flag variable to mark processId in logs
        extraLogs (str)[default:""]: extra string for logging
    Returns:
        None
    """
    format_list = ['%(levelname)s']
    if timeStamp_fl:
        format_list.append('%(asctime)s')
    if processId_fl:
        format_list.append('%(process)d')
    if extraLogs:
        format_list.append(extraLogs)

    
    format_list.append(' : %(message)s')

    # _format = '%(levelname)s: %(asctime)s %(process)d %(message)s '
    _format = " ".join(format_list)
    reload(logging) # https://stackoverflow.com/a/53553516
    logging.basicConfig(level=level,
                        format="{}".format(_format),
                        datefmt='%d/%m/%Y %I:%M:%S %p'
                        # filename=constants.LOG_FILE_LOCATION, filemode='a')
                        )
    return logging




class RMQ:
    """
    Used for interacting wiht RabbitMQ.
    Args:
        host (str): host for connection
        port (int): port as int
        user (str): user_name
        pwd (str): password
        socket_timeout (float) [default:None]: socket connect timeout in seconds
        stack_timeout (float) [default:None]: full protocol stack TCP/[SSL]/AMQP bring-up timeout in seconds
        heartbeat [default:0] : AMQP connection heartbeat timeout value for negotiation during connection tuning or callable which is invoked during connection tuning
    Returns:
        None
    Methods:
        listen() : for consuming messages
        publish() : for sending mesages
    """
    def __init__(self, url="", host="", port="", user="", pwd="", socket_timeout=None, stack_timeout=None, heartbeat=0):

        if url:
            self.channel = self.set_connection_url(url, socket_timeout, stack_timeout, heartbeat)
        else:
            self.channel = self.set_connection(host, port, user, pwd, socket_timeout, stack_timeout, heartbeat)

        self.cust_prop = pika.BasicProperties(
            delivery_mode=2,  # make message persistent
        )

    def set_connection_url(self, url, socket_timeout, stack_timeout, heartbeat):

        parameters = pika.URLParameters(url)
        parameters.socket_timeout = socket_timeout
        parameters.stack_timeout = stack_timeout
        parameters.heartbeat = heartbeat

        self.connection = pika.BlockingConnection(parameters)
        
        logging.info('[G-utils]--- Connection build successfully [URL] ---')

        return self.connection.channel()

    def set_connection(self, host, port, user, pwd, socket_timeout, stack_timeout, heartbeat):
        credentials = pika.PlainCredentials(user, pwd)
        parameters = pika.ConnectionParameters(host=host,
                                               port=port,
                                               virtual_host='/',
                                               socket_timeout=socket_timeout,
                                               stack_timeout=stack_timeout,
                                               heartbeat=heartbeat,
                                               credentials=credentials)
        self.connection = pika.BlockingConnection(parameters)
        
        logging.info('[G-utils]--- Connection build successfully [HOST-PORT]---')

        return self.connection.channel()

    
    def ack_message(self, channel, delivery_tag, thread_cnt):
        """Note that `channel` must be the same pika channel instance via which
        the message being ACKed was retrieved (AMQP protocol constraint).
        """
        if channel.is_open:
            channel.basic_ack(delivery_tag)
            logging.info("[G-utils]--- Joining thread(index): {} from messageThreads and deleting from stack".format(thread_cnt))
            self.messageThreads[thread_cnt].join()
            self.messageThreads.pop(thread_cnt, None)
            logging.info("[G-utils]--- Channel opened.. So message acknowledged")
        else:
            # Channel is already closed, so we can't ACK this message;
            logging.error("[G-utils]--- Channel closed.. So message NOT acknowledged")

    def do_work(self, connection, channel, delivery_tag, body, thread_cnt):
        thread_id = threading.get_ident()
        logging.info("[G-utils]--- Thread info= Thread id: {} Delivery tag: {}".format(thread_id, delivery_tag))
        logging.info("[G-utils]--- Message body: {}".format(body))

        # callback function initiated
        self.callback_func(body)
        
        cb = functools.partial(self.ack_message, channel, delivery_tag, thread_cnt)
        self.connection.add_callback_threadsafe(cb)

    def on_message(self, channel, method_frame, header_frame, body, args):
        (connection, threads) = args
        delivery_tag = method_frame.delivery_tag
        thread_key = str(uuid.uuid4())
        t = threading.Thread(target=self.do_work, args=(connection, channel, delivery_tag, body, thread_key))
        
        self.messageThreads[thread_key] = t
        logging.info("[G-utils]--- Threading fired with key: {}".format(thread_key))
        t.start()


    def callback(self, ch, method, properties, body):
        '''
        NOT in use
        '''
        logging.info("[G-utils]------------------ Message recieved : {}".format(body))
        self.callback_func(body)
        logging.info("[G-utils] Callback Terminated ============================================================================================= \n\n")


    def listen(self, consume_queue, callback_func, prefetch_count=1):
        """
        Used for listening to messages (continuous).
        Args:
            consume_queue (str): queue_name to listen
            callback_func (function): pass a funtion which takes a str as input(the message recieved)
        Returns:
            None [continuously waits for messages, never terminate]
        """
        self.callback_func = callback_func
        # try:
        #     self.channel.queue_declare(queue=consume_queue)
        # except Exception as e:
        #     logging.warning(' Queue Declaration error: {}'.format(e))

        self.channel.basic_qos(prefetch_count=prefetch_count)

        self.messageThreads = {}
        on_message_callback = functools.partial(self.on_message, args=(self.connection, self.messageThreads))

        self.channel.basic_consume(queue=consume_queue,
                                #    auto_ack=True,
                                   on_message_callback=on_message_callback)
        logging.info('[G-utils]------------------ Waiting for messages...')
        
        
        try:
            self.channel.start_consuming()
        except KeyboardInterrupt:
            logging.error("[G-utils]--- KeyboardInterrupt")
            self.channel.stop_consuming()
        except Exception as e:
            logging.error("[G-utils]--- Channel closed with error : {}".format(e))
            self.channel.stop_consuming()
        except:
            logging.error("[G-utils]--- Channel closed")
            self.channel.stop_consuming()

        # Wait for all to complete
        total_unfinished_threads = len(self.messageThreads)
        logging.info("[G-utils]--- Waiting for {} threads to complete".format(total_unfinished_threads))
        for cnt, (key,thread) in enumerate(self.messageThreads.items()):
            logging.info("[G-utils]---  Joining thread : {} and cnt:{}/{}".format(key, cnt+1, total_unfinished_threads))
            thread.join()


        try:
            self.connection.close()
            logging.info("[G-utils]--- Connection Closed")
        except Exception as e:
            logging.error("[G-utils]--- Connection closing failed with error:{}".format(e))

    def publish(self, msg_dict, publish_queue):
        """
        Used for publishing messages.
        Args:
            msg_dict (dict): message to publish (dict)
            publish_queue (str): queue_name to publish
        Returns:
            None
        """
        self.channel.basic_publish(exchange='',
                                   routing_key=publish_queue,
                                   properties=self.cust_prop,
                                   body=json.dumps(msg_dict))
        logging.info("[G-utils]------------------ Message published in queue {}!!!".format(publish_queue))



class REDIS:
    """
    Used for interacting wiht Redis.
    Args:
        url (str) : connection url
        host (str): host for connection
        port (int): port as int
        decode_responses (bool): used to binary str decode of hash values
    Returns:
        None
    Methods:
        get_data() : for fetching data corresponding to a redis-key
        get_hashData() : for fetching hashed data corresponding to a redis-key (entire hash/ value corresponding to a specific key)
        set_data() : for insert data corresponding to a redis-key
        check_key_exists() : for checking specific redis-key exists or not
    """

    def __init__(self, url="", host="", port="", decode_responses=False):
        self.context = pa.default_serialization_context()
        if url:
            self.redis_conn = self.set_connection_url(url, decode_responses)
        else:
            self.redis_conn = self.set_connection(host, port, decode_responses)

    def set_connection_url(self, url, decode_responses):
        if decode_responses:
            return redis.Redis.from_url(url, decode_responses=decode_responses)
        else:
            return redis.Redis.from_url(url)

    def set_connection(self, host, port, decode_responses):
        if decode_responses:
            return redis.Redis(
                    host=host,
                    port=port,
                    decode_responses=decode_responses
                )
        else:
            return redis.Redis(
                    host=host,
                    port=port)

    def get_data(self, redis_key, data_type="dict"):
        """
        Used for fetching data from Redis corresponding to a specific key (using 'get' method).
        Args:
            redis_key (str): uniquu redis-key
            data_type (str)[default:'dict']: type of data stored with the redis-key (options:dict, df) 
        Returns:
            Data (df/dict)
        """
        if data_type == "df":
            return self.context.deserialize(self.redis_conn.get("{}".format(redis_key)))
        elif data_type == "dict":
            return json.loads(self.redis_conn.get("{}".format(redis_key)))
        else:
            logging.error("[G-utils] Wrong data_type requested: data_type='df'/'dict'")
            return {}

    def get_hashData(self, redis_key, data_type="hash", hash_keys=""):
        """
        Used for fetching hashed data from Redis corresponding to a specific key.
        Args:
            redis_key (str): uniquu redis-key
            data_type (str)[default:'hash'] {options: hash/hash-values}: type of data fetched Eg: entire hash OR corresponding to a key(sinlge/multiple) 
            hash_keys(str/list): the key used in hash to fetch values
        Returns:
            Data (dict/string/list)
        """
        if data_type == "hash":
            return self.redis_conn.hgetall(redis_key)
        elif data_type == "hash-values":
            if hash_keys:
                if isinstance(hash_keys, str):
                    return self.redis_conn.hget(redis_key, hash_keys)
                elif isinstance(hash_keys, list):
                    return self.redis_conn.hmget(redis_key, hash_keys)
                else:
                    logging.error("[G-utils] Hash keys should be of type list/str")
                    return {}
            else:
                logging.error("[G-utils] Hash keys/key required of type list/str")
                return {}
        else:
            logging.error("[G-utils] Wrong data_type requested: data_type='hash'/'hash-values'")
            return {}

    def set_data(self, redis_key, data, data_type="dict", expiry=None):
        """
        Used for inserting data to Redis corresponding to a specific key.
        Args:
            redis_key (str): uniquu redis-key
            data (dict/list/df): data to be stored in redis
            data_type (str)[default:'dict']: type of data stored with the redis-key (options:dict, df, hash_multi) 
            expiry (str)[default:None]: set key expiry in sec (default no expiry) 
        Returns:
            Data (df/dict)
        """
        if data_type == "df":
            return self.redis_conn.set("{}".format(redis_key), self.context.serialize(data).to_buffer().to_pybytes(), ex=expiry)
        elif data_type == "dict":
            return self.redis_conn.set("{}".format(redis_key), json.dumps(data), ex=expiry)
        elif data_type == "hash_multi":
            return self.redis_conn.hmset("{}".format(redis_key), data)
        else:
            logging.error("[G-utils] Wrong data_type requested: data_type='df'/'dict'")
            return False

    def check_key_exists(self, redis_key):
        """
        Used for checking if key exists in Redis db.
        Args:
            redis_key (str): unique redis-key
        Returns:
            Bool (bool): based on key exist  
        """
        return self.redis_conn.exists(redis_key)


class FileStorage:
    """
    Used for interacting wiht MinIO.
    Args:
        url (str): url for connection
        user (str): user name
        pwd (str): password
    Returns:
        None
    Methods:
        get_data() : for fetching file corresponding to a folderName and fileName
        putFile() : for inserting file corresponding to a folderName and fileName
        get_folderLists() : fetch all File-names available in the given folder
    """
    def __init__(self, url, user, pwd):
        self.minioClient = self.set_connection(url, user, pwd)

    def set_connection(self, url, user, pwd):
        minioClient = Minio(url,
                            access_key=user,
                            secret_key=pwd,
                            secure=False)

        return minioClient

    def get_data(self, bucket_name, file_name, data_type="csv", index_col=None):
        """
        Used for fetching a file from MinIO from a specific bucket.
        Args:
            bucket_name (str): MinIO bucket name
            file_name (str): unique file name 
            data_type (str)[default:'csv']: type of data to read (csv/xlsx)
            index_col (int)[default:None]: column-index which is to be taken as the df-index 
        Returns:
            Data (df/dict)
        """
        data = self.minioClient.get_object(bucket_name, file_name)

        if data_type == "csv":
            if index_col is None:
                return pd.read_csv(data)
            else:
                return pd.read_csv(data, index_col=index_col)
        elif data_type == "xlsx":
            if index_col is None:
                return pd.read_excel(io.BytesIO(data.read()))
            else:
                return pd.read_excel(io.BytesIO(data.read()), index_col=index_col)
        else:
            logging.error("[G-utils] Wrong data_type requested: data_type='csv'")
            return {}

    def get_folderLists(self):
        """
        Used for fetching all bucket-names from MinIO.
        Args:
            None 
        Returns:
            Generator of bucket-names with creation dates
        """
        buckets = self.minioClient.list_buckets()
        for bucket in buckets:
            yield bucket.__dict__
            
    def putFile(self, bucket_name, storeFile_name, file_path, data_type="csv"):
        """
        Used for fetching a file from MinIO from a specific bucket.
        Args:
            bucket_name (str): MinIO bucket name
            storeFile_name (str): unique file name to be stored in MinIO
            file_path (str): path of file stored in local 
            data_type (str)[default:'csv']: type of data to read(csv/xlsx)
        Returns:
            etag and version ID if available.
        """
        if data_type=="csv":
            return self.minioClient.fput_object(
                        bucket_name=bucket_name, 
                        object_name=storeFile_name, 
                        file_path=file_path, 
                        content_type='application/csv'
                    )
        elif data_type=="xlsx":
            return self.minioClient.fput_object(
                        bucket_name=bucket_name, 
                        object_name=storeFile_name, 
                        file_path=file_path, 
                        content_type='application/vnd.openxmlformats-officedocument.spreadsheetml.sheet'
                    )
        else:
            logging.error("[G-utils] Wrong data_type requested: data_type='csv'")
            return {}


class APIrequest:
    """
    Used for requesting API.
    Args:
        None
    Returns:
        None
    Methods:
        get() : for GET rqst
        post() : for POST rqst
        put() : for PUT rqst
        get_multi() : for parallel GET rqst
        post_multi() : for parallel POST rqst
    """
    
    def __init__(self):
        self.header_content = {'Content-Type': 'application/json'}

    def post(self, url, body, headers={}, timeout_time=1800):
        """
        Used for POST request.
        Args:
            url (str): url
            body (dict): data dict  
            headers (dict)[default:{'Content-Type': 'application/json'}]: if required
            timeout_time(int)[default:1800]: timeout required 
        Returns:
            Response object
        """
        try:
            return requests.post(url, json.dumps(body), headers=dict(self.header_content, **headers), verify = False, timeout=timeout_time)
        except requests.ConnectionError as e:
            logging.error("[G-utils]   -API POST connection error[{}] ".format(e))
        except requests.Timeout as e:
            logging.error("[G-utils]   -API POST timeout error[{}]".format(e))

        return

    def get(self, url, headers={}, timeout_time=1800):
        """
        Used for GET request.
        Args:
            url (str): url
            headers (dict)[default:{'Content-Type': 'application/json'}]: if required
            timeout_time(int)[default:1800]: timeout required 
        Returns:
            Response object
        """
        try:
            return requests.get(url, headers=dict(self.header_content, **headers), verify = False, timeout=timeout_time)
        except requests.ConnectionError as e:
            logging.error("[G-utils]   -API POST connection error[{}] ".format(e))
        except requests.Timeout as e:
            logging.error("[G-utils]   -API POST timeout error[{}]".format(e))

        return

    def put(self, url, body, headers={}, timeout_time=1800):
        """
        Used for PUT request.
        Args:
            url (str): url
            body (dict): data dict  
            headers (dict)[default:{'Content-Type': 'application/json'}]: if required
            timeout_time(int)[default:1800]: timeout required 
        Returns:
            Response object
        """
        try:
            return requests.put(url, json.dumps(body), headers=dict(self.header_content, **headers), verify = False, timeout=timeout_time)
        except requests.ConnectionError as e:
            logging.error("[G-utils]   -API PUT connection error[{}] ".format(e))
        except requests.Timeout as e:
            logging.error("[G-utils]   -API PUT timeout error[{}]".format(e))

        return


    def post_multi(self, url, body_list, headers={}, timeout_time=1800, pool_size=1):
        """
        Used for parallel async POST request.
        Args:
            url (str): url
            body_list (list): list of data dicts  
            headers (dict)[default:{'Content-Type': 'application/json'}]: if required
            timeout_time(int)[default:1800]: timeout required 
            pool_size(int)[default:1]: number of parallel requests fired
        Returns:
            Generator of Response object
        """
        
        return grequests.imap( 
                        (grequests.post(url = url, data = json.dumps(individ_data) ,headers = dict(self.header_content, **headers), verify = False, timeout=timeout_time) for individ_data in body_list), 
                        size = pool_size, exception_handler=lambda request, exception: logging.error('[G-utils]   --Getting Multi POST response Failed with Timeout Exception [{} ] '.format(exception)))

    def get_multi(self, url_list, headers={}, timeout_time=1800, pool_size=1):
        """
        Used for parallel async GET request.
        Args:
            url_list (list): list of urls
            headers (dict)[default:{'Content-Type': 'application/json'}]: if required
            timeout_time(int)[default:1800]: timeout required 
            pool_size(int)[default:1]: number of parallel requests fired
        Returns:
            Generator of Response object
        """

        return grequests.imap( 
                        (grequests.get(url = individ_url,headers = dict(self.header_content, **headers), verify = False, timeout=timeout_time) for individ_url in url_list), 
                        size = pool_size, exception_handler=lambda request, exception: logging.error('[G-utils]   --Getting Multi GET response Failed with Timeout Exception [{} ]'.format(exception)))




