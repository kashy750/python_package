from gevent import monkey as curious_george
curious_george.patch_all(thread=False, select=False)

import logging
from importlib import reload # https://stackoverflow.com/a/53553516
import pika
from minio import Minio
import redis
import pyarrow as pa
import grequests, requests

import pandas as pd
import json



def logger():
    """
    Used for logging in cmd line.
    Args:
        None
    Returns:
        None
    """
    reload(logging) # https://stackoverflow.com/a/53553516
    logging.basicConfig(level=logging.INFO,
                        format='%(levelname)s: %(asctime)s %(process)d %(message)s ',
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
    def __init__(self, host, port, user, pwd, socket_timeout=None, stack_timeout=None, heartbeat=0):
        self.channel = self.set_connection(host, port, user, pwd, socket_timeout, stack_timeout, heartbeat)
        self.cust_prop = pika.BasicProperties(
            delivery_mode=2,  # make message persistent
        )

    def set_connection(self, host, port, user, pwd, socket_timeout, stack_timeout, heartbeat):
        credentials = pika.PlainCredentials(user, pwd)
        parameters = pika.ConnectionParameters(host=host,
                                               port=port,
                                               virtual_host='/',
                                               socket_timeout=socket_timeout,
                                               stack_timeout=stack_timeout,
                                               heartbeat=heartbeat,
                                               credentials=credentials)
        connection = pika.BlockingConnection(parameters)
        
        logging.info('--- Connection build successfully ---')

        return connection.channel()

    def callback(self, ch, method, properties, body):
        logging.info(" Message recieved : {}".format(body))
        redisId_dict = self.callback_func(body)
        if redisId_dict and self.publish_fl and self.publish_queue:
            self.publish(redisId_dict, self.publish_queue)
        logging.info(" Done Processing")
        logging.info(" XXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXX \n\n")


    def listen(self, consume_queue, callback_func, publish_fl=False, publish_queue=""):
        """
        Used for listening to messages (continuous).
        Args:
            consume_queue (str): queue_name to listen
            callback_func (function): pass a funtion which takes a str as input(the message recieved)
            publish_fl (bool)[default:True]: 'True', if some message to publish after callback_func()
            publish_queue (str)[default:""]: queue_name to publish
        Returns:
            None [continuously waits for messages, never terminate]
        """
        self.callback_func = callback_func
        self.publish_fl = publish_fl
        self.publish_queue = publish_queue
        # try:
        #     self.channel.queue_declare(queue=consume_queue)
        # except Exception as e:
        #     logging.warning(' Queue Declaration error: {}'.format(e))
        self.channel.basic_consume(queue=consume_queue,
                                   auto_ack=True,
                                   on_message_callback=self.callback)
        logging.info(' Waiting for messages...')
        self.channel.start_consuming()

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
        logging.info(" Redis keys send to solver for optimization!!!")


class REDIS:
    """
    Used for interacting wiht Redis.
    Args:
        host (str): host for connection
        port (int): port as int
    Returns:
        None
    Methods:
        get_data() : for fetching data corresponding to a redis-key
        set_data() : for insert data corresponding to a redis-key
    """

    def __init__(self, host, port):
        self.context = pa.default_serialization_context()
        self.redis_conn = self.set_connection(host, port)

    def set_connection(self, host, port):
        return redis.Redis(
            host=host,
            port=port)

    def get_data(self, redis_key, data_type="dict"):
        """
        Used for fetching data from Redis corresponding to a specific key.
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
            logging.error(" Wrong data_type requested: data_type='df'/'dict'")
            return {}

    def set_data(self, redis_key, data, data_type="dict"):
        """
        Used for inserting data to Redis corresponding to a specific key.
        Args:
            redis_key (str): uniquu redis-key
            redis_key (str): uniquu redis-key
            data_type (str)[default:'dict']: type of data stored with the redis-key (options:dict, df) 
        Returns:
            Data (df/dict)
        """
        if data_type == "df":
            return self.redis_conn.set("{}".format(redis_key), self.context.serialize(data).to_buffer().to_pybytes())
        elif data_type == "dict":
            return self.redis_conn.set("{}".format(redis_key), json.dumps(data))
        else:
            logging.error(" Wrong data_type requested: data_type='df'/'dict'")
            return False


class FileStorage:
    """
    Used for interacting wiht Redis.
    Args:
        host (str): url for connection
        user (str): user name
        pwd (str): password
    Returns:
        None
    Methods:
        get_data() : for fetching data corresponding to a redis-key
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
            data_type (str)[default:'csv']: type of data to read
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
        else:
            logging.error(" Wrong data_type requested: data_type='csv'")
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
        get_multi() : for parallel GET rqst
        post_multi() : for parallel POST rqst
    """
    
    def __init__(self):
        self.header_content = {'Content-Type': 'application/json'}

    def post(self, url, body, headers={}, timeout_time=600):
        """
        Used for POST request.
        Args:
            url (str): url
            body (dict): data dict  
            headers (dict)[default:{'Content-Type': 'application/json'}]: if required
            timeout_time(int)[default:600]: timeout required 
        Returns:
            Response object
        """
        try:
            return requests.post(url, json.dumps(body), headers=dict(self.header_content, **headers), verify = False, timeout=timeout_time)
        except requests.ConnectionError as e:
            logging.error("   -API POST connection error[{}] ".format(e))
        except requests.Timeout as e:
            logging.error("   -API POST timeout error[{}]".format(e))

        return

    def get(self, url, headers={}, timeout_time=600):
        """
        Used for GET request.
        Args:
            url (str): url
            headers (dict)[default:{'Content-Type': 'application/json'}]: if required
            timeout_time(int)[default:600]: timeout required 
        Returns:
            Response object
        """
        try:
            return requests.get(url, headers=dict(self.header_content, **headers), verify = False, timeout=timeout_time)
        except requests.ConnectionError as e:
            logging.error("   -API POST connection error[{}] ".format(e))
        except requests.Timeout as e:
            logging.error("   -API POST timeout error[{}]".format(e))

        return

    def post_multi(self, url, body_list, headers={}, timeout_time=600, pool_size=1):
        """
        Used for parallel async POST request.
        Args:
            url (str): url
            body_list (list): list of data dicts  
            headers (dict)[default:{'Content-Type': 'application/json'}]: if required
            timeout_time(int)[default:600]: timeout required 
            pool_size(int)[default:1]: number of parallel requests fired
        Returns:
            Generator of Response object
        """
        
        return grequests.imap( 
                        (grequests.post(url = url, data = json.dumps(individ_data) ,headers = dict(self.header_content, **headers), verify = False, timeout=timeout_time) for individ_data in body_list), 
                        size = pool_size, exception_handler=lambda request, exception: logging.error('   --Getting Multi POST response Failed with Timeout Exception [{} ] '.format(exception)))

    def get_multi(self, url_list, headers={}, timeout_time=600, pool_size=1):
        """
        Used for parallel async GET request.
        Args:
            url_list (list): list of urls
            headers (dict)[default:{'Content-Type': 'application/json'}]: if required
            timeout_time(int)[default:600]: timeout required 
            pool_size(int)[default:1]: number of parallel requests fired
        Returns:
            Generator of Response object
        """

        return grequests.imap( 
                        (grequests.get(url = individ_url,headers = dict(self.header_content, **headers), verify = False, timeout=timeout_time) for individ_url in url_list), 
                        size = pool_size, exception_handler=lambda request, exception: logging.error('   --Getting Multi GET response Failed with Timeout Exception [{} ]'.format(exception)))




