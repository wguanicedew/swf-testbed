#!/usr/bin/env python3
"""
EJFAT streaming-mode handler for the Fast Processing Agent.

Pipeline: handle_run_imminent_ejfat reserves a load balancer for the run;
handle_stf_ready_ejfat mirrors handle_stf_ready_activemq (simulate_tf_subsamples
-> record_tf_file -> _create_tf_slices), but instead of pushing each TFSlice to
the PanDA transformer queue, streams it as one event to the EJFAT load balancer
via e2sar_py's DataPlane Segmenter. As with the ActiveMQ path, each TFSlice is
marked terminal asynchronously when its slice_result message arrives (see
FastProcessingAgent.handle_slice_result / _finalize_fastmon_file_if_terminal),
not by this module. The load balancer is freed once
fast_processing_utils.check_run_finalized reports every FastMonFile sampled
during the run is terminal (see _finalize_run_if_terminal_ejfat /
handle_end_run_ejfat) -- not on any single TF file's completion, since a run
typically samples many.

Reference for the e2sar_py control/data-plane API shape: ../E2SAR/python/client.py
"""

import os
from datetime import datetime

import fast_processing_utils


def ejfat_reserve_load_balancer(agent, message_data):
    """
    Reserve an EJFAT load balancer for this run, then open and start the
    data-plane Segmenter (registering as a sender on the LB) used to stream
    TF slice events to it for the rest of the run.
    """
    try:
        import e2sar_py
    except ImportError as e:
        raise RuntimeError(
            "streaming_mode='ejfat' requires the e2sar_py package (EJFAT/E2SAR Python "
            "bindings, https://github.com/JeffersonLab/E2SAR) to be installed and importable."
        ) from e

    ejfat_config = agent.config.get('ejfat', {})
    admin_uri_str = ejfat_config.get('admin_uri') or os.environ.get('EJFAT_URI')
    if not admin_uri_str:
        raise RuntimeError(
            "streaming_mode='ejfat' requires an admin EJFAT URI to reserve a load balancer: "
            "set 'admin_uri' in the [ejfat] config section or the EJFAT_URI environment variable."
        )

    admin_uri = e2sar_py.EjfatURI(uri=admin_uri_str, tt=e2sar_py.EjfatURI.TokenType.admin)
    lbm = e2sar_py.ControlPlane.LBManager(admin_uri, not ejfat_config.get('insecure', False))

    run_id = message_data.get('run_id') or agent.current_run_id
    lb_name = ejfat_config.get('lb_name') or f"{agent.agent_name}-{run_id}"
    duration = ejfat_config.get('lb_duration_seconds', 3600)

    result = lbm.reserve_lb_in_seconds(
        lb_id=lb_name,
        seconds=float(duration),
        senders=ejfat_config.get('senders', []),
        ip_family=ejfat_config.get('ip_family', 0),
    )
    if result.has_error():
        raise RuntimeError(f"Failed to reserve EJFAT load balancer: {result.error().message()}")

    agent._ejfat_lbm = lbm
    instance_uri = lbm.get_uri()
    agent.logger.info(
        f"EJFAT load balancer '{lb_name}' reserved for {duration}s",
        extra=agent._log_extra()
    )

    sflags = e2sar_py.DataPlane.Segmenter.SegmenterFlags()
    sflags.useCP = ejfat_config.get('use_cp', True)
    sflags.rateGbps = ejfat_config.get('rate_gbps', -1.0)
    sflags.mtu = ejfat_config.get('mtu', 0)

    data_id = ejfat_config.get('data_id', 0)
    event_src_id = ejfat_config.get('event_src_id', 0)

    sender_lbm = None
    if sflags.useCP:
        sender_lbm = e2sar_py.ControlPlane.LBManager(instance_uri, not ejfat_config.get('insecure', False))
        result = sender_lbm.add_sender_self(False)
        if result.has_error():
            raise RuntimeError(f"Failed to add sender to EJFAT load balancer: {result.error().message()}")

    segmenter = e2sar_py.DataPlane.Segmenter(instance_uri, data_id, event_src_id, sflags)
    result = segmenter.OpenAndStart()
    if result.has_error():
        raise RuntimeError(f"Failed to start EJFAT segmenter: {result.error().message()}")

    agent._ejfat_segmenter = segmenter
    agent._ejfat_sender_lbm = sender_lbm
    agent.logger.info(
        f"EJFAT segmenter started (data_id={data_id}, event_src_id={event_src_id}, mtu={segmenter.getMTU()})"
    )


def ejfat_free_load_balancer(agent):
    """
    Stop the data-plane segmenter and free the reserved EJFAT load balancer,
    if one was reserved. Safe to call more than once.
    """
    segmenter = getattr(agent, '_ejfat_segmenter', None)
    if segmenter is not None:
        segmenter.stopThreads()
        agent._ejfat_segmenter = None

    sender_lbm = getattr(agent, '_ejfat_sender_lbm', None)
    if sender_lbm is not None:
        try:
            sender_lbm.remove_sender_self(False)
        except Exception:
            pass
        agent._ejfat_sender_lbm = None

    lbm = getattr(agent, '_ejfat_lbm', None)
    if lbm is None:
        return
    try:
        result = lbm.free_lb()
        if result.has_error():
            agent.logger.error(f"Failed to free EJFAT load balancer: {result.error().message()}")
        else:
            agent.logger.info("EJFAT load balancer freed", extra=agent._log_extra())
    except Exception as e:
        agent.logger.error(f"Error freeing EJFAT load balancer: {e}", extra=agent._log_extra(error=str(e)))
    finally:
        agent._ejfat_lbm = None

def ejfat_send_event(agent, payload):
    """
    Send one event's payload through the EJFAT segmenter opened by
    ejfat_reserve_load_balancer. Returns True on success, False on failure (logged).
    """
    segmenter = getattr(agent, '_ejfat_segmenter', None)
    if segmenter is None:
        raise RuntimeError(
            "EJFAT segmenter not started -- handle_run_imminent_ejfat must reserve the "
            "load balancer before events can be sent"
        )

    result = segmenter.sendEvent(payload, len(payload))
    if result.has_error():
        agent.logger.error(f"Failed to send EJFAT event: {result.error().message()}",
                            extra=agent._log_extra())
        return False
    return True


def ejfat_receive_event(agent, wait_ms=200):
    """
    Receive one event from the EJFAT reassembler.

    No reassembler lifecycle management (registerWorker/OpenAndStart) lives in
    this module yet -- a worker/reassembler counterpart is expected to open an
    e2sar_py.DataPlane.Reassembler and cache it on agent._ejfat_reassembler,
    the same way ejfat_reserve_load_balancer caches the sender-side segmenter on
    agent._ejfat_segmenter.

    Returns (event_bytes, event_num, data_id), or None if nothing was received
    within wait_ms or on error (logged).
    """
    reassembler = getattr(agent, '_ejfat_reassembler', None)
    if reassembler is None:
        raise RuntimeError(
            "EJFAT reassembler not started -- open one and cache it on "
            "agent._ejfat_reassembler before calling receive_event"
        )

    recv_len, recv_bytes, event_num, data_id = reassembler.recvEventBytes(wait_ms=wait_ms)
    if recv_len == -2:
        agent.logger.error("EJFAT receive error", extra=agent._log_extra())
        return None
    if recv_len == -1:
        return None

    return recv_bytes, event_num, data_id


def handle_run_imminent_ejfat(agent, message_data):
    """
    Handle run_imminent when streaming_mode == 'ejfat': reserve a load balancer
    for the upcoming run. EJFAT workers (reassemblers) register directly with
    the load balancer's control plane rather than listening on
    WORKER_BROADCAST_TOPIC, so this skips the ActiveMQ worker broadcast
    handle_run_imminent_activemq sends.
    """
    agent.logger.info(
        f"Run imminent (ejfat): execution_id={agent.current_execution_id}, run_id={agent.current_run_id}",
        extra=agent._log_extra()
    )

    workflow_params = agent._get_workflow_params(
        message_data.get('run_id') or agent.current_run_id,
        message_data.get('execution_id') or agent.current_execution_id
    )
    fast_processing = workflow_params.get('fast_processing', {})

    agent._log_system_event('run_imminent', {
        'execution_id': agent.current_execution_id,
        'streaming_mode': 'ejfat',
        'stf_sampling_rate': fast_processing.get('stf_sampling_rate', 0),
        'no_duplicate_mode': fast_processing.get('no_duplicate_mode', False)
    })

    try:
        ejfat_reserve_load_balancer(agent, message_data)
    except Exception as e:
        agent.logger.error(
            f"Failed to reserve EJFAT load balancer on run_imminent: {e}",
            extra=agent._log_extra(error=str(e))
        )


def _handle_slice_ejfat(agent, message_data, fast_processing=None):
    """Create TF slices (same DB bookkeeping as the ActiveMQ path) and stream each one to EJFAT as an event."""
    fast_processing = fast_processing or {}
    tf_filename = message_data.get('tf_filename')
    tf_file_id = message_data.get('tf_file_id')
    stf_filename = message_data.get('stf_filename')
    tf_first = message_data.get('tf_first', 0)
    tf_last = message_data.get('tf_last')
    tf_count = message_data.get('tf_count')
    run_id = message_data.get('run_id')

    agent.logger.info(
        f"Handling TF sub sample via EJFAT: {tf_filename} (tf_first={tf_first}, tf_last={tf_last}, tf_count={tf_count})",
        extra=agent._log_extra(tf_filename=tf_filename, stf_filename=stf_filename)
    )

    num_tf_per_slice = fast_processing.get('num_tf_per_slice', agent.config.get('tfs_per_subsample', 2))

    # Create TF slices from this TF sample; one slice becomes one EJFAT event.
    slices = agent._create_tf_slices(
        run_id, tf_filename, tf_file_id, stf_filename, tf_first, tf_last, tf_count, num_tf_per_slice, dest_path=None
    )

    event_size_bytes = agent.config.get('ejfat', {}).get('event_size_bytes', 1024)

    events_sent = 0
    for slice_data in slices:
        payload = os.urandom(event_size_bytes)
        if ejfat_send_event(agent, payload):
            events_sent += 1
        else:
            agent.logger.error(
                f"Failed to send slice {slice_data.get('slice_id')} of {tf_filename} via EJFAT",
                extra=agent._log_extra(tf_filename=tf_filename)
            )

    agent._update_run_state_slices(run_id=run_id, new_slices_count=len(slices))

    agent._log_system_event('tf_file_processed', {
        'tf_filename': tf_filename,
        'stf_filename': stf_filename,
        'slices_created': len(slices),
        'events_sent': events_sent,
        'streaming_mode': 'ejfat'
    })

    with agent._state_lock:
        agent.stats['slices_created'] += len(slices)
        agent.stats['slices_sent'] += events_sent
        agent.stats['tf_files_processed'] += 1
        agent.tf_files_processed += 1


def handle_stf_ready_ejfat(agent, message_data):
    """
    Handle stf_ready message when streaming_mode == 'ejfat'.

    Mirrors handle_stf_ready_activemq: samples TF sub-samples from the STF via
    simulate_tf_subsamples and records each as a FastMonFile. Where the
    ActiveMQ path pushes each resulting TFSlice to the PanDA transformer
    queue, this streams each one as one EJFAT event (see _handle_slice_ejfat).
    """
    agent.logger.info("Processing stf_ready message (ejfat)", extra=agent._log_extra())

    run_id = message_data.get('run_id')

    with agent._state_lock:
        agent.last_message_time = datetime.now()
        agent.stf_messages_processed += 1
        agent.stats['total_stf_messages'] += 1
        force_sample = run_id not in agent.runs_sampled

    tf_files_processed = []
    if not message_data.get('filename'):
        agent.logger.error("No filename provided in message", extra=agent._log_extra())
        return tf_files_processed

    workflow_params = agent._get_workflow_params(
        message_data.get('run_id') or agent.current_run_id,
        message_data.get('execution_id') or agent.current_execution_id
    )
    fast_processing = workflow_params.get('fast_processing', {})

    tf_subsamples = fast_processing_utils.simulate_tf_subsamples(
        message_data, fast_processing, agent.config, agent.logger, agent.agent_name,
        force_sample=force_sample
    )

    if tf_subsamples and run_id:
        with agent._state_lock:
            agent.runs_sampled[run_id] = datetime.now()
            agent._expire_runs_sampled()

    tf_files_created = 0
    no_duplicate_mode = agent.config.get('no_duplicate_mode', False)
    for tf_metadata in tf_subsamples:
        tf_file = fast_processing_utils.record_tf_file(tf_metadata, agent.config, agent, agent.logger)
        if tf_file:
            tf_files_created += 1
            already_registered = tf_file.get('_already_registered', False)
            if not (no_duplicate_mode and already_registered):
                tf_file_id = tf_file.get('tf_file_id')

                tf_sub_message = {
                    'tf_filename': tf_file.get('tf_filename'),
                    'tf_file_id': tf_file_id,
                    'stf_filename': tf_file.get('stf_file') or message_data.get('filename'),
                    'tf_first': tf_file.get('tf_first'),
                    'tf_last': tf_file.get('tf_last'),
                    'tf_count': tf_file.get('tf_count'),
                    'file_type': message_data.get('file_type'),
                    'run_id': message_data.get('run_id'),
                    'execution_id': message_data.get('execution_id') or agent.current_execution_id,
                }
                _handle_slice_ejfat(agent, tf_sub_message, fast_processing)
                fast_processing_utils.update_tf_file_status(
                    tf_file_id, fast_processing_utils.FileStatus.PROCESSING, agent, agent.logger
                )
        tf_files_processed.append(tf_file)

    with agent._state_lock:
        agent.stats['tf_files_created'] += tf_files_created

    agent.logger.info(
        f"Processed {tf_files_created} TF sub samples via EJFAT",
        extra=agent._log_extra(stf_filename=message_data.get('filename'), tf_files_created=tf_files_created)
    )
    return tf_files_processed


def handle_end_run_ejfat(agent, message_data):
    """
    Handle end_run when streaming_mode == 'ejfat'.

    Frees the load balancer immediately if every FastMonFile sampled during
    the run has already reached a terminal state, otherwise
    _finalize_run_if_terminal_ejfat frees it once the last outstanding one
    finalizes. Skips the ActiveMQ worker broadcast handle_end_run_activemq
    sends, since EJFAT workers aren't listening on it.
    """
    total_stf = message_data.get('total_stf_files', 0)

    agent.logger.info(
        f"Run ended (ejfat): run_id={message_data.get('run_id') or agent.current_run_id}, "
        f"tf_files_processed={agent.stats['tf_files_processed']}, "
        f"slices_created={agent.stats['slices_created']}",
        extra=agent._log_extra(total_stf=total_stf,
                                tf_files_processed=agent.stats['tf_files_processed'],
                                slices_created=agent.stats['slices_created'])
    )

    agent._update_run_state(run_id=message_data.get('run_id'), phase='completed', state='ended', substate=None)

    agent._log_system_event('end_run', {
        'execution_id': agent.current_execution_id,
        'streaming_mode': 'ejfat',
        'total_tf_files_processed': agent.stats['tf_files_processed'],
        'total_slices_created': agent.stats['slices_created'],
        'total_slices_sent': agent.stats['slices_sent']
    })

    run_id = message_data.get('run_id') or agent.current_run_id
    if fast_processing_utils.check_run_finalized(run_id, agent, agent.logger):
        ejfat_free_load_balancer(agent)

    # Clear current run state
    agent.current_run_id = None
    agent.current_execution_id = None
    agent.workflow_params = {}

    # Agent is now idle, waiting for next run
    agent.set_ready()


def _finalize_run_if_terminal_ejfat(agent, tf_file_id):
    """
    Called from the shared _finalize_fastmon_file_if_terminal once a TF file's
    slices have all reached a terminal state. Checks the database for every
    FastMonFile sampled during the run (not just this one) and frees the load
    balancer once all of them are terminal -- a run typically samples many TF
    files across many stf_ready messages, so this single TF file finalizing
    doesn't necessarily mean the run is done.
    """
    run_id = agent.current_run_id
    if fast_processing_utils.check_run_finalized(run_id, agent, agent.logger):
        agent.logger.info(
            f"All FastMonFiles for run={run_id} are terminal, freeing EJFAT load balancer",
            extra=agent._log_extra(tf_file_id=tf_file_id)
        )
        ejfat_free_load_balancer(agent)
