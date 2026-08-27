"""
Fast Processing Agent: Creates TF slices from STF samples for PanDA workers.

This agent:
1. Receives tf_file_registered messages from FastMon Agent (via epictopic)
2. Creates TF slices from STF samples
3. Pushes TF slices to PanDA transformer queue (/queue/panda.transformer.slices)
4. Maintains RunState and TFSlice records in the monitor database

Pipeline: FastMon Agent [tf_file_registered] -> Fast Processing Agent [TF slices] -> PanDA Workers

Message format specification: https://github.com/wguanicedew/iDDS/blob/dev/main/prompt.md
"""

import os
import signal
import time
import logging
import threading
import traceback
import json
import uuid
from datetime import datetime, timedelta, timezone
import stomp
import tomllib
from swf_common_lib.base_agent import BaseAgent
import fast_processing_utils


class FastProcessingAgent(BaseAgent):
    """
    Fast Processing Agent for TF slice creation and distribution.

    Subscribes to epictopic, receives stf_ready from Data Agent,
    creates TF slices, and pushes them to the PanDA transformer queue.
    """

    # Queue for transformer workers (from Wen's iDDS design)
    TRANSFORMER_QUEUE = '/topic/panda.slices'

    # Queue for worker broadcasts
    WORKER_BROADCAST_TOPIC = '/topic/panda.workers'

    # Queue for transformer results
    TRANSFORMER_RESULTS_QUEUE = '/queue/panda.results.fastprocessing'

    def __init__(self, debug=False, config_path=None, dest_path=None):
        super().__init__(
            agent_type='Fast_Processing',
            subscription_queue='/topic/epictopic',
            debug=debug,
            config_path=config_path
        )
        self.default_dest_path = dest_path
        # Additional subscriptions beyond the primary queue (subscribed in run())
        self._extra_subscription_queues = [self.TRANSFORMER_RESULTS_QUEUE]

        # Workflow parameters (populated on run_imminent)
        self.workflow_params = {}

        # Cache: run_id -> {'params': dict, 'expires_at': datetime}
        self.workflow_params_cache = {}

        # Processing state
        self.tf_files_processed = 0
        self.slices_created = 0
        self.stf_messages_processed = 0
        self.last_message_time = None
        self.runs_sampled = {}  # run_id -> datetime when first sampled

        # handle_stf_ready/handle_slice_result run concurrently on the background
        # worker pool (see on_message). This guards only the shared counter/dict
        # updates (stats, runs_sampled, workflow_params_cache) those handlers
        # touch — the blocking REST calls in between stay unlocked so the pool's
        # multiple workers can actually overlap.
        self._state_lock = threading.Lock()

        # Statistics
        self.stats = {
            'total_stf_messages': 0,
            'tf_files_created': 0,
            'tf_files_processed': 0,
            'slices_created': 0,
            'slices_sent': 0,
            'results_received': 0,
            'results_done': 0,
            'results_failed': 0
        }

        # Default configuration
        self.config = {
            "stf_sampling_rate": 0.1,  # 10% of files
            # TF simulation parameters
            "tf_files_per_stf": 7,  # Number of TF files to generate per STF
            "tf_size_fraction": 0.15,  # Fraction of partition TF count per subsample (with gaussian noise)
            "tf_count_per_stf": 1000,  # Default total TF count per STF if not provided in stf_ready message
            "tf_sequence_start": 1,  # Starting sequence number for TF files
            "no_duplicate_mode": False,  # Set True to skip notification for already-registered TF files
            "run_id_lifetime": 2,       # Days to keep run_id in runs_sampled cache
            "streaming_mode": "activemq",  # 'activemq' (default) or 'ejfat'
            "tfs_per_subsample": 20,  # Number of TFs per subsample file
            # EJFAT streaming parameters (used only when streaming_mode == 'ejfat'),
            # populated from the [ejfat] section of the config file. Recognized keys:
            # uri, data_id, event_src_id, use_cp, rate_gbps, mtu, event_size_bytes.
            "ejfat": {},
            # Worker sizing (broadcast to PanDA transformer workers on run_imminent)
            "target_worker_count": 1,
            "memory_per_core": 4000,
            "slice_processing_time": 10,
            "worker_rampup_time": 1,
            "worker_rampdown_time": 1,
            # Transformer job parameters
            "epic_version": None,
            "epic_image": None,
            "processor_type": None,
            "dest_path": None,
        }

        # Overwrite defaults with values from [fast_processing] section of config file
        config_path = self.config_path
        if config_path is None:
            env_config = os.getenv('SWF_TESTBED_CONFIG')
            config_path = env_config if env_config else 'workflows/testbed.toml'
        try:
            with open(config_path, 'rb') as f:
                toml_data = tomllib.load(f)
            file_config = toml_data.get('fast_processing', {})
            self.config.update({k: v for k, v in file_config.items() if k in self.config})
            ejfat_config = toml_data.get('ejfat', {})
            self.config['ejfat'] = ejfat_config
        except FileNotFoundError:
            pass

    def run(self):
        """
        Override run() to subscribe to both the workflow topic and the
        transformer results queue before entering the main loop.
        """
        def signal_handler(signum, frame):
            sig_name = signal.Signals(signum).name
            logging.info(f"Received {sig_name}, initiating graceful shutdown...")
            raise KeyboardInterrupt(f"Received {sig_name}")

        signal.signal(signal.SIGTERM, signal_handler)
        signal.signal(signal.SIGQUIT, signal_handler)

        logging.info(f"Starting {self.agent_name}...")

        # Connect to ActiveMQ
        if not getattr(self, 'mq_connected', False):
            max_retries = 3
            retry_delay = 5
            for attempt in range(1, max_retries + 1):
                logging.info(f"Connecting to ActiveMQ at {self.mq_host}:{self.mq_port} (attempt {attempt}/{max_retries})")
                try:
                    self.conn.connect(
                        self.mq_user,
                        self.mq_password,
                        wait=True,
                        version='1.1',
                        headers={
                            'client-id': self.agent_name,
                            'heart-beat': '30000,30000'
                        }
                    )
                    self.mq_connected = True
                    break
                except Exception as e:
                    logging.warning(f"Connection attempt {attempt} failed: {e}")
                    if attempt < max_retries:
                        logging.info(f"Retrying in {retry_delay} seconds...")
                        time.sleep(retry_delay)
                    else:
                        logging.error(f"Failed to connect after {max_retries} attempts")
                        raise

        try:
            # Subscribe to primary workflow topic
            self.conn.subscribe(destination=self.subscription_queue, id=1, ack='auto')
            logging.info(f"Subscribed to queue: '{self.subscription_queue}'")

            # Subscribe to all extra queues (e.g. transformer results)
            for idx, queue in enumerate(self._extra_subscription_queues, start=2):
                self.conn.subscribe(destination=queue, id=idx, ack='auto')
                logging.info(f"Subscribed to queue: '{queue}'")

            # Register all subscriptions in monitor
            self._register_subscribers()

            # Agent is now ready and waiting for work
            self.set_ready()

            self.send_heartbeat()

            logging.info(f"{self.agent_name} is running. Press Ctrl+C to stop.")
            while True:
                time.sleep(60)
                if not self.mq_connected:
                    self._attempt_reconnect()
                self.send_heartbeat()

        except KeyboardInterrupt:
            logging.info(f"Stopping {self.agent_name}...")
        except stomp.exception.ConnectFailedException as e:
            self.mq_connected = False
            logging.error(f"Failed to connect to ActiveMQ: {e}")
            logging.error("Please check the connection details and ensure ActiveMQ is running.")
        except Exception as e:
            self.mq_connected = False
            logging.error(f"An unexpected error occurred: {e}")
            traceback.print_exc()
        finally:
            try:
                self.operational_state = 'EXITED'
                self.send_heartbeat()
            except Exception:
                pass
            if self.config.get('streaming_mode') == 'ejfat':
                try:
                    from fast_processing_ejfat import ejfat_free_load_balancer
                    ejfat_free_load_balancer(self)
                except Exception:
                    pass
            try:
                if self.mq_connected:
                    self.conn.disconnect()
            except Exception:
                pass

    def _attempt_reconnect(self):
        """
        Override _attempt_reconnect to resubscribe to all queues
        (primary + extra) after reconnection.
        """
        if self.mq_connected:
            return True

        try:
            logging.info("Attempting to reconnect to ActiveMQ...")
            if self.conn.is_connected():
                self.conn.disconnect()

            self.conn.connect(
                self.mq_user,
                self.mq_password,
                wait=True,
                version='1.1',
                headers={
                    'client-id': self.agent_name,
                    'heart-beat': '30000,30000'
                }
            )

            # Resubscribe to primary queue
            self.conn.subscribe(destination=self.subscription_queue, id=1, ack='auto')
            logging.info(f"Resubscribed to queue: '{self.subscription_queue}'")

            # Resubscribe to all extra queues
            for idx, queue in enumerate(self._extra_subscription_queues, start=2):
                self.conn.subscribe(destination=queue, id=idx, ack='auto')
                logging.info(f"Resubscribed to queue: '{queue}'")

            self.mq_connected = True
            logging.info("Successfully reconnected to ActiveMQ")
            return True

        except Exception as e:
            logging.warning(f"Reconnection attempt failed: {e}")
            self.mq_connected = False
            return False

    def _expire_runs_sampled(self):
        """Remove run_ids older than run_id_lifetime days from runs_sampled."""
        lifetime_days = self.config.get('run_id_lifetime', 2)
        cutoff = datetime.now() - timedelta(days=lifetime_days)
        expired = [rid for rid, ts in self.runs_sampled.items() if ts < cutoff]
        for rid in expired:
            del self.runs_sampled[rid]
        if expired:
            self.logger.debug(f"Expired {len(expired)} run IDs from runs_sampled")

    def _register_subscribers(self):
        """Register all subscriptions (primary + extra) in the monitor."""
        all_queues = [self.subscription_queue] + self._extra_subscription_queues
        for queue in all_queues:
            self._register_single_subscriber(queue)

    def _register_single_subscriber(self, queue):
        """Register a single subscription in the monitor API."""
        subscriber_data = {
            'subscriber_name': f"{self.agent_name}-{queue}",
            'description': f"{self.agent_type} agent subscribing to {queue}",
            'is_active': True,
            'fraction': 1.0
        }
        try:
            result = self._api_request('post', '/subscribers/', subscriber_data)
            if result:
                if result.get('status') == 'already_exists':
                    logging.info(f"Subscriber already registered: {subscriber_data['subscriber_name']}")
                else:
                    logging.info(f"Subscriber registered: {subscriber_data['subscriber_name']}")
        except Exception as e:
            logging.error(f"Failed to register subscriber for {queue}: {e}")

    def send_message(self, destination, message_body, headers=None):
        """
        Override BaseAgent.send_message to add optional STOMP headers support.

        The base agent calls conn.send(body, destination) with no headers.
        This override merges caller-supplied headers with sensible defaults and
        passes them through to the broker.

        Args:
            destination: ActiveMQ destination ('/queue/...' or '/topic/...')
            message_body: Dict to send as JSON. 'sender'/'namespace' auto-injected
                          by the base class logic replicated here.
            headers: Optional dict of additional STOMP headers, e.g.
                     {'persistent': 'true', 'ttl': '43200000'}
        """
        if not destination.startswith('/queue/') and not destination.startswith('/topic/'):
            raise ValueError(
                f"destination must start with '/queue/' or '/topic/', got: '{destination}'. "
                f"Use '/queue/{destination}' for anycast or '/topic/{destination}' for multicast."
            )

        # Mirror base agent: auto-inject sender and namespace
        message_body['sender'] = self.agent_name
        if self.namespace:
            message_body['namespace'] = self.namespace
        else:
            logging.warning(
                f"Sending message without namespace (msg_type={message_body.get('msg_type', 'unknown')}). "
                "Configure namespace in testbed.toml to enable namespace filtering."
            )

        # Auto-inject created_at if not already set by the caller
        if 'created_at' not in message_body:
            message_body['created_at'] = datetime.utcnow().isoformat()

        # Build STOMP headers: start with defaults, merge caller overrides on top
        run_id = message_body.get('run_id') or self.current_run_id
        stomp_headers = {
            'persistent': 'false',
            'vo': 'eic',
            'msg_type': message_body.get('msg_type', 'unknown'),
            'namespace': message_body.get('namespace', 'default'),
            'run_id': str(run_id) if run_id else 'none',
        }
        if headers:
            stomp_headers.update(headers)

        try:
            self.conn.send(
                body=json.dumps(message_body),
                destination=destination,
                headers=stomp_headers
            )
            logging.info(f"Sent message to '{destination}' | headers={stomp_headers} | body={message_body}")
        except Exception as e:
            logging.error(f"Failed to send message to '{destination}': {e}")
            if any(t in str(e).lower() for t in ['ssl', 'eof', 'connection', 'broken pipe']):
                logging.warning("Connection error detected - attempting recovery")
                self.mq_connected = False
                time.sleep(1)
                if self._attempt_reconnect():
                    try:
                        self.conn.send(
                            body=json.dumps(message_body),
                            destination=destination,
                            headers=stomp_headers
                        )
                        logging.info(f"Message sent after reconnection to '{destination}' | headers={stomp_headers} | body={message_body}")
                    except Exception as retry_e:
                        logging.error(f"Retry failed after reconnection: {retry_e}")
                else:
                    logging.error("Reconnection failed - message lost")

    def on_message(self, frame):
        """Handle incoming workflow messages."""
        message_data, msg_type = self.log_received_message(frame)
        if message_data is None:
            return

        # Extract run context from each message (agents may start mid-run)
        self._update_run_context(message_data)

        try:
            if msg_type == 'run_imminent':
                self.handle_run_imminent(message_data)
            elif msg_type == 'start_run':
                self.handle_start_run(message_data)
            elif msg_type == 'stf_ready':
                # Offloaded: loops over TF sub-samples doing blocking REST calls
                # (record_tf_file, update_tf_file_status, ...) per stf_ready message.
                self.run_in_background(self.handle_stf_ready, message_data, label='stf_ready')
            elif msg_type == 'pause_run':
                self.handle_pause_run(message_data)
            elif msg_type == 'resume_run':
                self.handle_resume_run(message_data)
            elif msg_type == 'end_run':
                self.handle_end_run(message_data)
            elif msg_type == 'slice_result':
                # Offloaded: updates the TFSlice record via a blocking REST call.
                self.run_in_background(self.handle_slice_result, message_data, label='slice_result')
            else:
                self.logger.debug(f"Ignoring message type: {msg_type}")
        except Exception as e:
            self.logger.error(f"Error processing {msg_type}: {e}",
                              extra=self._log_extra(error=str(e)))
            self.logger.error(traceback.format_exc())

    def _update_run_context(self, message_data):
        """
        Update run context from message. Agents may start mid-run and miss run_imminent,
        so we extract run_id/execution_id from every message and fetch params if needed.
        """
        run_id = message_data.get('run_id')
        execution_id = message_data.get('execution_id')

        # Update current run context if provided
        if run_id and run_id != self.current_run_id:
            self.current_run_id = run_id
            # Reset stats for new run
            self.tf_files_processed = 0
            self.slices_created = 0
            self.stats = {
                'total_stf_messages': 0,
                'tf_files_created': 0,
                'tf_files_processed': 0,
                'slices_created': 0,
                'slices_sent': 0,
                'results_received': 0,
                'results_done': 0,
                'results_failed': 0
            }

        if execution_id and (execution_id != self.current_execution_id or not self.workflow_params):
            self.current_execution_id = execution_id
            # Fetch workflow params if we don't have them (use cache keyed by run_id)
            if not self.workflow_params:
                self.workflow_params = self._get_workflow_params(run_id, execution_id)
                if self.workflow_params:
                    self.logger.info(f"Workflow parameters loaded (mid-run): {json.dumps(self.workflow_params, indent=2, sort_keys=True)}")

    def handle_run_imminent(self, message_data):
        """Dispatch run_imminent handling based on the configured streaming_mode."""
        if self.config.get('streaming_mode') == 'ejfat':
            from fast_processing_ejfat import handle_run_imminent_ejfat
            return handle_run_imminent_ejfat(self, message_data)
        return self.handle_run_imminent_activemq(message_data)
    
    def handle_run_imminent_activemq(self, message_data):
        """Handle run_imminent message."""
        self.logger.info(
            f"Run imminent: execution_id={self.current_execution_id}, run_id={self.current_run_id}",
            extra=self._log_extra()
        )

        workflow_params = self._get_workflow_params(
            message_data.get('run_id') or self.current_run_id,
            message_data.get('execution_id') or self.current_execution_id
        )
        fast_processing = workflow_params.get("fast_processing", {})

        self._log_system_event('run_imminent', {
            'execution_id': self.current_execution_id,
            'target_worker_count': self.config.get('target_worker_count', 0),
            'stf_sampling_rate': fast_processing.get('stf_sampling_rate', 0),
            'slices_per_sample': fast_processing.get('slices_per_sample', 0),
            'no_duplicate_mode': fast_processing.get('no_duplicate_mode', False)
        })

        # Build and broadcast a run_imminent message to workers
        try:
            # Compose message similar to _send_slice_to_queue format.
            # Put the incoming message_data inside 'content' and add execution_id
            # and target_worker_count so workers know how many to spin up.
            content = dict(message_data or {})
            content.update({
                'execution_id': self.current_execution_id,
                'core_count': self.config.get('target_worker_count', 1),
                'memory_per_core': self.config.get('memory_per_core', 4000),
                'target_worker_count': self.config.get('target_worker_count', 1),
                'slice_processing_time': self.config.get('slice_processing_time', 1),
                'worker_rampup_time': self.config.get('worker_rampup_time', 1),
                'worker_rampdown_time': self.config.get('worker_rampdown_time', 1)
            })

            run_id = message_data.get('run_id') or self.current_run_id
            message = {
                'msg_type': 'run_imminent_worker',
                'run_id': run_id,
                'created_at': datetime.utcnow().isoformat(),
                'content': content
            }

            # Topic for worker broadcasts
            worker_topic = self.WORKER_BROADCAST_TOPIC
            self.send_message(worker_topic, message)

            self.logger.info(f"Broadcasted run_imminent to workers: {worker_topic}",
                             extra=self._log_extra(destination=worker_topic))
        except Exception as e:
            self.logger.error(f"Failed to broadcast run_imminent to workers: {e}",
                              extra=self._log_extra(error=str(e)))

    def handle_start_run(self, message_data):
        """Handle start_run: Update RunState phase to 'physics'."""
        self.logger.info(f"Run started: run_id={self.current_run_id}",
                         extra=self._log_extra())

        # Agent is now actively processing this run
        self.set_processing()

        self._update_run_state(run_id=message_data.get('run_id'), phase='physics', state='running', substate='physics')

        self._log_system_event('start_run', {
            'execution_id': self.current_execution_id
        })

    def handle_stf_ready(self, message_data):
        """Dispatch stf_ready handling based on the configured streaming_mode."""
        if self.config.get('streaming_mode') == 'ejfat':
            from fast_processing_ejfat import handle_stf_ready_ejfat
            return handle_stf_ready_ejfat(self, message_data)
        return self.handle_stf_ready_activemq(message_data)

    def handle_stf_ready_activemq(self, message_data):
        """
        Handle stf_ready message and sample STFs into TFs
        Registers the TFs in the swf-monitor database and notifies clients.
        """
        self.logger.info("Processing stf_ready message", extra=self._log_extra())

        run_id = message_data.get('run_id')

        # Update message tracking stats
        with self._state_lock:
            self.last_message_time = datetime.now()
            self.stf_messages_processed += 1
            self.stats['total_stf_messages'] += 1

            force_sample = run_id not in self.runs_sampled

        tf_files_processed = []
        self.logger.debug(f"Message data received: {message_data}", extra=self._log_extra())
        if not message_data.get('filename'):
            self.logger.error("No filename provided in message", extra=self._log_extra())
            return tf_files_processed

        # Get num_tf_per_slice from workflow params
        workflow_params = self._get_workflow_params(
            message_data.get('run_id') or self.current_run_id,
            message_data.get('execution_id') or self.current_execution_id
        )
        fast_processing = workflow_params.get('fast_processing', {})

        tf_subsamples = fast_processing_utils.simulate_tf_subsamples(message_data, fast_processing,self.config, self.logger, self.agent_name,
                                                                     force_sample=force_sample)

        if tf_subsamples and run_id:
            with self._state_lock:
                self.runs_sampled[run_id] = datetime.now()
                self._expire_runs_sampled()

        # Record each TF file in the FastMonFile table
        # TODO: register in bulk
        tf_files_created = 0
        no_duplicate_mode = self.config.get('no_duplicate_mode', False)
        self.logger.debug(f"Simulated {len(tf_subsamples)} TF sub samples")
        for tf_metadata in tf_subsamples:
            self.logger.debug(f"Processing TF sub sample: {tf_metadata}")
            tf_file = fast_processing_utils.record_tf_file(tf_metadata, self.config, self, self.logger)
            if tf_file:
                tf_files_created += 1
                already_registered = tf_file.get('_already_registered', False)
                if not (no_duplicate_mode and already_registered):
                    tf_sub_message = {
                        'tf_filename': tf_file.get('tf_filename'),
                        'tf_file_id': tf_file.get('tf_file_id'),
                        'stf_filename': tf_file.get('stf_file') or message_data.get('filename'),
                        'tf_first': tf_file.get('tf_first'),
                        'tf_last': tf_file.get('tf_last'),
                        'tf_count': tf_file.get('tf_count'),
                        'file_type': message_data.get('file_type'),
                        'run_id': message_data.get('run_id'),
                        'execution_id': message_data.get('execution_id') or self.current_execution_id,
                    }
                    self.handle_slice(tf_sub_message, fast_processing)
                    fast_processing_utils.update_tf_file_status(
                        tf_file.get('tf_file_id'), fast_processing_utils.FileStatus.PROCESSING, self, self.logger
                    )
            tf_files_processed.append(tf_file)

        # Update TF creation stats
        with self._state_lock:
            self.stats['tf_files_created'] += tf_files_created


        # self.logger.info(f"Processed {tf_files_created} TF sub samples for STF file {message_data.get('filename')}",
        #                  extra=self._log_extra(stf_filename=message_data.get('filename'), tf_files_created=tf_files_created))
        self.logger.info(f"Processed {tf_files_created} TF sub samples",
                         extra=self._log_extra(stf_filename=message_data.get('filename'), tf_files_created=tf_files_created))
        return tf_files_processed
    
    def handle_slice(self, message_data, fast_processing=None):
        """
        Handle TF sub sample: Create TF slices, push to worker queue.
        """
        fast_processing = fast_processing or {}
        tf_filename = message_data.get('tf_filename')
        tf_file_id = message_data.get('tf_file_id')
        stf_filename = message_data.get('stf_filename')
        tf_first = message_data.get('tf_first', 0)
        tf_last = message_data.get('tf_last')
        tf_count = message_data.get('tf_count')
        file_type = message_data.get('file_type')

        # self.logger.info(f"Handling TF sub sample: {tf_filename} (from STF: {stf_filename}, tf_first={tf_first}, tf_last={tf_last}, tf_count={tf_count})",
        #                  extra=self._log_extra(tf_filename=tf_filename, stf_filename=stf_filename))
        self.logger.info(f"Handling TF sub sample: {tf_filename} (tf_first={tf_first}, tf_last={tf_last}, tf_count={tf_count})",
                         extra=self._log_extra(tf_filename=tf_filename, stf_filename=stf_filename))
        

        num_tf_per_slice = fast_processing.get('num_tf_per_slice', self.config.get('tfs_per_subsample', 2))
        
        epic_image, epic_version, processor_type = fast_processing_utils.resolve_epic_params(
            fast_processing, self.config, self.logger
        )
        dest_path = fast_processing.get('dest_path', self.config.get('dest_path', None)) or self.default_dest_path

        run_id = message_data.get('run_id')

        # Create TF slices from this TF sample
        slices = self._create_tf_slices(run_id, tf_filename, tf_file_id, stf_filename, tf_first, tf_last, tf_count, num_tf_per_slice, dest_path)

        # Push each slice to transformer queue
        for slice_data in slices:
            self._send_slice_to_queue(run_id, slice_data, epic_version=epic_version, epic_image=epic_image, processor_type=processor_type, file_type=file_type)

        # Update RunState with slice counts
        self._update_run_state_slices(run_id=run_id, new_slices_count=len(slices))

        # Log event
        self._log_system_event('tf_file_processed', {
            'tf_filename': tf_filename,
            'stf_filename': stf_filename,
            'slices_created': len(slices)
        })

        with self._state_lock:
            self.stats['slices_created'] += len(slices)
            self.stats['tf_files_processed'] += 1
            self.tf_files_processed += 1

    def handle_pause_run(self, message_data):
        """Handle pause_run: Update RunState to standby."""
        self.logger.info(f"Run paused: run_id={message_data.get('run_id') or self.current_run_id}",
                         extra=self._log_extra())

        self._update_run_state(run_id=message_data.get('run_id'), substate='standby')

        self._log_system_event('pause_run', {
            'execution_id': self.current_execution_id
        })

    def handle_resume_run(self, message_data):
        """Handle resume_run: Update RunState back to physics."""
        self.logger.info(f"Run resumed: run_id={message_data.get('run_id') or self.current_run_id}",
                         extra=self._log_extra())

        self._update_run_state(run_id=message_data.get('run_id'), substate='physics')

        self._log_system_event('resume_run', {
            'execution_id': self.current_execution_id
        })

    def handle_end_run(self, message_data):
        """Dispatch end_run handling based on the configured streaming_mode."""
        if self.config.get('streaming_mode') == 'ejfat':
            from fast_processing_ejfat import handle_end_run_ejfat
            return handle_end_run_ejfat(self, message_data)
        return self.handle_end_run_activemq(message_data)

    def handle_end_run_activemq(self, message_data):
        """Handle end_run: Update RunState to completed."""
        total_stf = message_data.get('total_stf_files', 0)

        self.logger.info(
            f"Run ended: run_id={message_data.get('run_id') or self.current_run_id}, "
            f"tf_files_processed={self.stats['tf_files_processed']}, "
            f"slices_created={self.stats['slices_created']}",
            extra=self._log_extra(total_stf=total_stf,
                                  tf_files_processed=self.stats['tf_files_processed'],
                                  slices_created=self.stats['slices_created'])
        )

        self._update_run_state(run_id=message_data.get('run_id'), phase='completed', state='ended', substate=None)

        self._log_system_event('end_run', {
            'execution_id': self.current_execution_id,
            'total_tf_files_processed': self.stats['tf_files_processed'],
            'total_slices_created': self.stats['slices_created'],
            'total_slices_sent': self.stats['slices_sent']
        })

        # Broadcast end_run to workers so they can perform any teardown/cleanup
        try:
            # Compose message similar to _send_slice_to_queue format.
            # Put the incoming message_data inside 'content' and add execution_id
            # and target_worker_count so workers can finalize appropriately.
            content = dict(message_data or {})
            content.update({
                'execution_id': self.current_execution_id
            })

            run_id = message_data.get('run_id') or self.current_run_id
            message = {
                'msg_type': 'end_run',
                'run_id': run_id,
                'created_at': datetime.utcnow().isoformat(),
                'content': content
            }

            worker_topic = self.WORKER_BROADCAST_TOPIC
            self.send_message(worker_topic, message)

            self.logger.info(f"Broadcasted end_run to workers: {worker_topic}",
                             extra=self._log_extra(destination=worker_topic))
        except Exception as e:
            self.logger.error(f"Failed to broadcast end_run to workers: {e}",
                              extra=self._log_extra(error=str(e)))

        # Clear current run state
        self.current_run_id = None
        self.current_execution_id = None
        self.workflow_params = {}

        # Agent is now idle, waiting for next run
        self.set_ready()

    def handle_slice_result(self, message_data):
        """Process slice_result messages from transformer workers."""
        logging.info(f"Received slice_result message: {message_data}")
        with self._state_lock:
            self.stats['results_received'] += 1

        content = message_data.get('content', {})
        result = content.get('result') if isinstance(content, dict) else None

        self.logger.info(
            f"Slice result received: run={message_data.get('run_id')}, "
            f"state={content.get('state') if isinstance(content, dict) else 'unknown'}",
            extra=self._log_extra(run_id=message_data.get('run_id'))
        )

        # Track done/failed counts if result payload present
        try:
            inner_result = None
            if result and isinstance(result, dict):
                inner_result = result.get('result') if isinstance(result.get('result'), dict) else None

            state = content.get('state') or (inner_result.get('state') if inner_result else None)
            with self._state_lock:
                if state == 'done' or (inner_result and inner_result.get('processed')):
                    self.stats['results_done'] += 1
                else:
                    self.stats['results_failed'] += 1
        except Exception:
            pass

        # Update TFSlice record in database
        self._update_tfslice_from_result(message_data, content, result)

        # Log system event for observability
        self._log_system_event('slice_result', {
            'message': message_data,
            'state': content.get('state') if isinstance(content, dict) else None,
            'results_received': self.stats['results_received'],
            'results_done': self.stats['results_done'],
            'results_failed': self.stats['results_failed']
        })

        self.logger.info(f"Handled slice_result: run={message_data.get('run_id')}, msg={message_data.get('msg_type')}",
                         extra=self._log_extra(run_id=message_data.get('run_id')))

    # -------------------------------------------------------------------------
    # Helper methods
    # -------------------------------------------------------------------------

    def _get_workflow_params(self, run_id, execution_id=None):
        """Return workflow params for run_id from cache, fetching if missing or expired.

        On a cache hit the expiry is refreshed from the current access time.
        If execution_id is None, only the cache is consulted (no fetch).
        """
        now = datetime.now(timezone.utc)

        with self._state_lock:
            # Evict all stale entries on every access
            expired_keys = [k for k, v in self.workflow_params_cache.items() if now >= v['expires_at']]
            for k in expired_keys:
                del self.workflow_params_cache[k]
                self.logger.debug(f"Workflow params cache expired for run_id={k}")

            entry = self.workflow_params_cache.get(run_id)
            if entry:
                # Refresh expiry on access
                lifetime_hours = entry['params'].get('cache_lifetime_hours', 24)
                entry['expires_at'] = now + timedelta(hours=lifetime_hours)
                return entry['params']

        if execution_id is None:
            return {}

        # Fetch (blocking REST call) outside the lock so concurrent callers for
        # different run_ids don't serialize on it; only the cache write is locked.
        params = self._fetch_workflow_parameters(execution_id)
        if params:
            lifetime_hours = params.get('cache_lifetime_hours', 24)
            with self._state_lock:
                self.workflow_params_cache[run_id] = {
                    'params': params,
                    'expires_at': now + timedelta(hours=lifetime_hours)
                }
            self.logger.debug(f"Workflow params cached for run_id={run_id}, lifetime={lifetime_hours}h")
        return params

    def _fetch_workflow_parameters(self, execution_id):
        """Fetch workflow parameters from WorkflowExecution API."""
        try:
            result = self.call_monitor_api(
                'GET',
                f'/workflow-executions/{execution_id}/'
            )
            if result:
                return result.get('parameter_values', {})
            return {}
        except Exception as e:
            self.logger.error(f"Failed to fetch workflow parameters: {e}",
                              extra=self._log_extra(error=str(e)))
            return {}

    def _update_run_state(self, run_id=None, phase=None, state=None, substate=None):
        """Update RunState record."""
        update_data = {
            'state_changed_at': datetime.now().isoformat()
        }
        if phase is not None:
            update_data['phase'] = phase
        if state is not None:
            update_data['state'] = state
        if substate is not None:
            update_data['substate'] = substate

        try:
            result = self.call_monitor_api(
                'PATCH',
                f'/run-states/{run_id or self.current_run_id}/',
                update_data
            )
            if result:
                self.logger.debug(f"RunState updated: {update_data}", extra=self._log_extra())
        except Exception as e:
            self.logger.error(f"Error updating RunState: {e}",
                              extra=self._log_extra(error=str(e)))

    def _update_run_state_slices(self, run_id=None, new_slices_count=0):
        """Update RunState with new slice counts."""
        # We need to increment, so fetch current values first
        try:
            current = self.call_monitor_api('GET', f'/run-states/{run_id or self.current_run_id}/')
            if current:
                update_data = {
                    'stf_samples_received': current.get('stf_samples_received', 0) + 1,
                    'slices_created': current.get('slices_created', 0) + new_slices_count,
                    'slices_queued': current.get('slices_queued', 0) + new_slices_count,
                    'state_changed_at': datetime.now().isoformat()
                }
                self.call_monitor_api(
                    'PATCH',
                    f'/run-states/{self.current_run_id}/',
                    update_data
                )
        except Exception as e:
            self.logger.error(f"Error updating RunState slices: {e}",
                              extra=self._log_extra(error=str(e)))

    def _create_tf_slices(self, run_id, tf_filename, tf_file_id, stf_filename, tf_first, tf_last, tf_count, num_tf_per_slice, dest_path=None):
        """
        Create TF slice records in database, based on the TF file's range [tf_first, tf_last].

        Slices divide the TF file's range into chunks of num_tf_per_slice TFs each.
        Slice filenames are derived from tf_filename.

        Returns list of slice data dictionaries for sending to queue.
        """
        import math
        slices = []

        if tf_last is None or tf_count is None:
            self.logger.error(f"Missing tf_last or tf_count for {tf_filename} — cannot create slices",
                              extra=self._log_extra(tf_filename=tf_filename))
            return slices

        num_slices = math.ceil(tf_count / num_tf_per_slice)

        for i in range(num_slices):
            slice_tf_first = tf_first + i * num_tf_per_slice
            slice_tf_last = min(slice_tf_first + num_tf_per_slice - 1, tf_last)
            slice_tf_count = slice_tf_last - slice_tf_first + 1

            slice_data = {
                'slice_id': i,
                'tf_first': slice_tf_first,
                'tf_last': slice_tf_last,
                'tf_count': slice_tf_count,
                'tf_filename': tf_filename,
                'fastmon_file': tf_filename,  # TFSliceSerializer resolves this slug to the FastMonFile FK
                'tf_file_id': tf_file_id,
                'stf_filename': stf_filename,
                'dest_path': dest_path,
                'run_number': self.current_run_id,
                'run_id': run_id or self.current_run_id,
                'status': 'queued',
                'retries': 0,
                'metadata': {
                    'execution_id': self.current_execution_id,
                    'created_by': self.agent_name
                }
            }

            # Create in database
            try:
                workflow_params = self._get_workflow_params(run_id or self.current_run_id, self.current_execution_id)
                no_duplicate_mode = workflow_params.get("fast_processing", {}).get('no_duplicate_mode', False)
                existing = self.call_monitor_api('GET', f'/tf-slices/?fastmon_file_id={tf_file_id}&tf_filename={tf_filename}&slice_id={i}')
                if existing:
                    match = next((r for r in existing
                                  if r.get('tf_filename') == tf_filename and r.get('slice_id') == i), None)
                    if match:
                        self.logger.info(f"TFSlice {tf_filename} slice_id={i} already exists with ID {match.get('id')}, skipping",
                                         extra=self._log_extra(tf_filename=tf_filename))
                        if not no_duplicate_mode:
                            slice_data['db_id'] = match.get('id')
                            slices.append(slice_data)
                        continue

                result = self.call_monitor_api('POST', '/tf-slices/', slice_data)
                if result:
                    self.stats['slices_created'] += 1
                    self.slices_created += 1
                    # Add database ID to slice data for queue message
                    slice_data['db_id'] = result.get('id')
                    slices.append(slice_data)
                    self.logger.debug(f"TFSlice created: {tf_filename} slice_id={i} with ID {result.get('id')}",
                                      extra=self._log_extra(tf_filename=tf_filename))
                else:
                    self.logger.warning(f"Failed to create TFSlice: {tf_filename}",
                                        extra=self._log_extra(tf_filename=tf_filename))
            except Exception as e:
                self.logger.error(f"Error creating TFSlice {tf_filename}: {e}",
                                  extra=self._log_extra(tf_filename=tf_filename, error=str(e)))

        return slices

    def _send_slice_to_queue(self, run_id, slice_data, epic_version=None, epic_image=None, processor_type=None, file_type=None):
        """
        Send slice message to transformer queue.

        Message format per Wen's iDDS design.
        """
        # Build message per iDDS format
        content = {
            'run_id': run_id or self.current_run_id,
            'execution_id': self.current_execution_id,
            'req_id': str(uuid.uuid4()),
            'filename': slice_data['stf_filename'],
            'tf_filename': slice_data['tf_filename'],
            'tf_file_id': slice_data.get('tf_file_id'),
            'tf_slice_id': slice_data.get('db_id'),
            'slice_id': slice_data['slice_id'],
            'start': slice_data['tf_first'],
            'end': slice_data['tf_last'],
            'tf_count': slice_data['tf_count'],
            'dest_path': slice_data['dest_path'],
            'epic_version': epic_version,
            'epic_image': epic_image,
            'processor_type': processor_type,
            'state': 'queued',
            'substate': 'new'
        }
        if file_type is not None:
            content['file_type'] = file_type

        message = {
            'msg_type': 'slice',
            'run_id': run_id or self.current_run_id,
            'created_at': datetime.utcnow().isoformat(),
            'content': content
        }

        # Send to transformer queue — persistent so slices survive broker restart,
        # ttl of 12 hours so unprocessed slices are eventually discarded
        try:
            self.send_message(
                self.TRANSFORMER_QUEUE,
                message,
                headers={
                    'persistent': 'true',
                    'ttl': str(12 * 3600 * 1000)  # 12 hours in ms
                }
            )

            self.stats['slices_sent'] += 1
            self.logger.info(
                f"Slice sent to queue -> {self.TRANSFORMER_QUEUE}",
                extra=self._log_extra(tf_filename=slice_data['tf_filename'], destination=self.TRANSFORMER_QUEUE)
            )
        except Exception as e:
            self.logger.error(f"Failed to send slice to queue: {e}",
                              extra=self._log_extra(error=str(e)))

    def _log_system_event(self, event_type, event_data):
        """Log event to SystemStateEvent table."""
        workflow_params = self._get_workflow_params(self.current_run_id, self.current_execution_id)
        event = {
            'timestamp': datetime.now().isoformat(),
            'run_number': self.current_run_id,
            'event_type': event_type,
            'state': workflow_params.get('state', 'unknown'),
            'substate': workflow_params.get('substate'),
            'event_data': event_data
        }

        try:
            self.call_monitor_api('POST', '/system-state-events/', event)
        except Exception as e:
            self.logger.debug(f"Failed to log system event: {e}",
                              extra=self._log_extra(event_type=event_type, error=str(e)))

    def _update_tfslice_from_result(self, message_data, content, result):
        """Update TFSlice record in database based on slice_result message."""
        try:
            # Extract slice information from the result
            # The result structure is: content -> result -> result (nested)
            inner_result = None
            origin_message = None
            metrics = None
            payload_result = None
            if result and isinstance(result, dict):
                inner_result = result.get('result') if isinstance(result.get('result'), dict) else None
                origin_message = inner_result.get('origin_message') if inner_result and isinstance(inner_result, dict) else None
                metrics = inner_result.get('metrics') if inner_result and isinstance(inner_result, dict) else None
                payload_result = inner_result.get('payload_result') if inner_result and isinstance(inner_result, dict) else None

            # Get slice_id directly from the result data
            slice_id = None
            tf_filename = None
            tf_file_id = None
            tf_slice_id = None
            if origin_message and isinstance(origin_message, dict):
                slice_id = origin_message.get('slice_id')
                tf_filename = origin_message.get('tf_filename')
                tf_file_id = origin_message.get('tf_file_id')
                tf_slice_id = origin_message.get('tf_slice_id')

            if tf_slice_id is None:
                self.logger.debug("No tf_slice_id in result, cannot update TFSlice record")
                return

            # Determine the final state
            state = content.get('state') if isinstance(content, dict) else None
            processed = inner_result.get('processed') if inner_result else None

            # Map worker state to slice status
            if state == 'done' or processed:
                slice_status = 'completed'
            else:
                slice_status = 'failed'

            # Build update payload
            update_data = {
                'status': slice_status,
                'completed_at': content.get('processed_at') or datetime.now().isoformat(),
                'metadata': {
                    'metrics': metrics,
                    'payload_result': payload_result
                }
            }

            # Update the slice directly using tf_filename+slice_id from the message.
            # (tf_filename, slice_id) is the model's unique_together key: slice_id is
            # only a 0-14 serial within a TF file, so filtering by run_id+slice_id alone
            # is ambiguous once a run has more than one TF file.
            run_id = message_data.get('run_id')
            try:
                # Update the slice using database ID
                api_result = self.call_monitor_api(
                    'PATCH',
                    f'/tf-slices/{tf_slice_id}/',
                    update_data
                )
                if api_result:
                    self.logger.info(
                        f"TFSlice updated: tf_slice_id={tf_slice_id}, tf_filename={tf_filename} -> {slice_status}",
                        extra=self._log_extra(tf_slice_id=tf_slice_id, tf_filename=tf_filename, status=slice_status)
                    )
                    self._finalize_fastmon_file_if_terminal(tf_file_id)
                else:
                    self.logger.warning(
                        f"Failed to update TFSlice: tf_slice_id={tf_slice_id}",
                        extra=self._log_extra(tf_slice_id=tf_slice_id)
                    )

            except Exception as e:
                self.logger.error(
                    f"Error updating TFSlice tf_slice_id={tf_slice_id}: {e}",
                    extra=self._log_extra(tf_slice_id=tf_slice_id, error=str(e))
                )

        except Exception as e:
            self.logger.error(
                f"Error updating TFSlice from result: {e}",
                extra=self._log_extra(error=str(e))
            )

    def _finalize_fastmon_file_if_terminal(self, tf_file_id):
        """Roll up a FastMonFile's status once every one of its TFSlices has
        reached a terminal state: all completed -> DONE, all failed -> FAILED,
        a mix of the two -> PROCESSED. No-op while any slice is still
        queued/processing.
        """
        if not tf_file_id:
            return

        try:
            slices = self.call_monitor_api('GET', f'/tf-slices/?fastmon_file_id={tf_file_id}')
        except Exception as e:
            self.logger.error(f"Error fetching TFSlices for fastmon_file_id={tf_file_id}: {e}",
                              extra=self._log_extra(tf_file_id=tf_file_id, error=str(e)))
            return

        if not slices:
            return

        statuses = [s.get('status') for s in slices]
        if not all(status in ('completed', 'failed') for status in statuses):
            return

        if all(status == 'completed' for status in statuses):
            new_status = fast_processing_utils.FileStatus.DONE
        elif all(status == 'failed' for status in statuses):
            new_status = fast_processing_utils.FileStatus.FAILED
        else:
            new_status = fast_processing_utils.FileStatus.PROCESSED

        fast_processing_utils.update_tf_file_status(tf_file_id, new_status, self, self.logger)
        self.logger.info(
            f"FastMonFile {tf_file_id} finalized: status={new_status} ({len(statuses)} slices)",
            extra=self._log_extra(tf_file_id=tf_file_id, status=new_status)
        )

        self._finalize_run_if_terminal_ext(tf_file_id)
        
    def _finalize_run_if_terminal_ext(self, tf_file_id):
        """Finalize EJFAT based on the configured streaming_mode."""
        if self.config.get('streaming_mode') == 'ejfat':
            from fast_processing_ejfat import _finalize_run_if_terminal_ejfat
            return _finalize_run_if_terminal_ejfat(self, tf_file_id)


if __name__ == "__main__":
    import argparse
    from pathlib import Path

    script_dir = Path(__file__).parent

    parser = argparse.ArgumentParser(
        description="Fast Processing Agent - samples STFs and creates TF slices"
    )
    parser.add_argument("--debug", action="store_true", help="Enable debug logging")
    parser.add_argument("--testbed-config", default=None,
                        help="Testbed config file (default: SWF_TESTBED_CONFIG env var or workflows/testbed.toml)")
    parser.add_argument("--dest-path", default=None,
                        help="Default destination path for TF slices (used when dest_path is not set in workflow parameters)")
    args = parser.parse_args()

    agent = FastProcessingAgent(debug=args.debug, config_path=args.testbed_config, dest_path=args.dest_path)
    agent.run()
