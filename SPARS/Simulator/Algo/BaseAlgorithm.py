# =============================================================================
# INSTRUCTIONS FOR USE:
# 1. This file now contains a new, more advanced policy called `hybrid_policy`.
# 2. To use it, you must open your `easy_auto_switch.py` file.
# 3. In the `schedule` method of that file, change the line that calls the policy to:
#
#    super().hybrid_policy()
#
# 4. You will also need to set a `timeout` value in your `simulator_config.yaml`
#    (e.g., 300 seconds) because this policy uses both time and queue length.
# =============================================================================

import copy


class BaseAlgorithm():
    def __init__(self, state, waiting_queue, start_time, jobs_manager, timeout=None):
        self.state = state
        self.waiting_queue = waiting_queue
        self.events = []
        self.current_time = start_time
        self.timeout = timeout

        self.jobs_manager = jobs_manager

        self.available = []
        self.inactive = []
        self.compute_speeds = []
        self.allocated = []
        self.scheduled = []
        self.call_me_later = []
        self.timeout_list = []

    def push_event(self, timestamp, event):
        found = next(
            (x for x in self.events if x['timestamp'] == timestamp), None)
        if found:
            found['events'].append(event)
        else:
            self.events.append({'timestamp': timestamp, 'events': [event]})
        self.events.sort(key=lambda x: x['timestamp'])

    def set_time(self, current_time):
        self.current_time = current_time

    def remove_from_timeout_list(self, node_ids):
        ids = set(node_ids)
        self.timeout_list[:] = [ti for ti in self.timeout_list
                                if ti.get('node_id') not in ids]

    def timeout_policy(self):
        # This is the original, unmodified timeout policy.
        now = self.current_time
        t_exp = now + self.timeout
        allocated_ids = [node['id'] for node in self.allocated]
        timeout_node_ids = {t['node_id'] for t in self.timeout_list}
        for node in self.state:
            if (
                node['job_id'] is None
                and node['state'] == 'active'
                and node['reserved'] == False
                and node['id'] not in allocated_ids
                and node['id'] not in timeout_node_ids
            ):
                self.timeout_list.append(
                    {'node_id': node['id'], 'time': t_exp})
                timeout_node_ids.add(node['id'])
        state_by_id = {n['id']: n for n in self.state}
        allocated_ids = {n['id'] for n in self.allocated}
        switch_off = []
        keep_timeouts = []
        next_earliest = None
        for t in self.timeout_list:
            node = state_by_id.get(t['node_id'])
            if not node:
                continue
            if now < t['time'] or node['state'] != 'active':
                keep_timeouts.append(t)
                if next_earliest is None or t['time'] < next_earliest:
                    next_earliest = t['time']
                continue
            if node['job_id'] is None and node['id'] not in allocated_ids:
                switch_off.append(node['id'])
        self.timeout_list = keep_timeouts
        if switch_off:
            self.push_event(now, {'type': 'switch_off', 'nodes': switch_off})
        if next_earliest is not None and getattr(self, 'next_timeout_at', None) != next_earliest:
            self.push_event(next_earliest, {'type': 'call_me_later'})
            self.next_timeout_at = next_earliest

    def queue_aware_policy(self):
        # This is the previous custom policy. It is ineffective when the queue is always full.
        nodes_to_turn_off = []
        if len(self.waiting_queue) < 10:
            for node in self.available:
                nodes_to_turn_off.append(node['id'])
        if nodes_to_turn_off:
            self.push_event(self.current_time, {
                            'type': 'switch_off', 'nodes': nodes_to_turn_off})
        nodes_to_turn_on = []
        if len(self.waiting_queue) > 10:
            if self.inactive:
                num_to_turn_on = min(len(self.inactive), 5)
                for i in range(num_to_turn_on):
                    nodes_to_turn_on.append(self.inactive[i]['id'])
        if nodes_to_turn_on:
            self.push_event(self.current_time, {
                            'type': 'switch_on', 'nodes': nodes_to_turn_on})

    # =========================================================================
    # START: NEW HYBRID POWER STATE MANAGEMENT METHOD
    # =========================================================================
    def hybrid_policy(self):
        """
        This is a more advanced hybrid policy. Its rule is:
        - Switch off a node ONLY IF it has been idle for a certain `timeout`
          AND the system load is low (e.g., waiting queue has < 20 jobs).
        This prevents switching off nodes during busy periods, even if they are
        temporarily idle, which should create a better balance.
        """
        now = self.current_time
        t_exp = now + self.timeout

        allocated_ids = [node['id'] for node in self.allocated]
        timeout_node_ids = {t['node_id'] for t in self.timeout_list}

        for node in self.state:
            if (
                node['job_id'] is None
                and node['state'] == 'active'
                and node['reserved'] == False
                and node['id'] not in allocated_ids
                and node['id'] not in timeout_node_ids
            ):
                self.timeout_list.append(
                    {'node_id': node['id'], 'time': t_exp})
                timeout_node_ids.add(node['id'])

        state_by_id = {n['id']: n for n in self.state}
        allocated_ids = {n['id'] for n in self.allocated}

        switch_off = []
        keep_timeouts = []
        next_earliest = None

        for t in self.timeout_list:
            node = state_by_id.get(t['node_id'])
            if not node:
                continue

            if now < t['time'] or node['state'] != 'active':
                keep_timeouts.append(t)
                if next_earliest is None or t['time'] < next_earliest:
                    next_earliest = t['time']
                continue

            # This is the moment a node's idle timer has expired.
            # We now add the second condition before switching it off.
            if node['job_id'] is None and node['id'] not in allocated_ids:
                # --- HYBRID POLICY CHANGE ---
                # Check the system load by looking at the waiting queue length.
                # You can tune this "20" to find the best balance.
                if len(self.waiting_queue) < 20:
                    # If the queue is short, it's safe to switch the node off.
                    switch_off.append(node['id'])
                else:
                    # If the queue is long, the system is busy.
                    # We should keep the node ON and reset its timer for another cycle.
                    keep_timeouts.append(
                        {'node_id': node['id'], 'time': now + self.timeout})
                    if next_earliest is None or (now + self.timeout) < next_earliest:
                        next_earliest = now + self.timeout
                # --- END OF HYBRID POLICY CHANGE ---

        self.timeout_list = keep_timeouts
        if switch_off:
            self.push_event(now, {'type': 'switch_off', 'nodes': switch_off})
        if next_earliest is not None and getattr(self, 'next_timeout_at', None) != next_earliest:
            self.push_event(next_earliest, {'type': 'call_me_later'})
            self.next_timeout_at = next_earliest
    # =========================================================================
    # END: NEW HYBRID POWER STATE MANAGEMENT METHOD
    # =========================================================================

    def prep_schedule(self, new_state, waiting_queue, scheduled_queue, resources_agenda):
        self.state = new_state
        self.waiting_queue = waiting_queue
        self.scheduled_queue = scheduled_queue
        self.resources_agenda = copy.deepcopy(resources_agenda)
        self.resources_agenda = sorted(
            self.resources_agenda,
            key=lambda x: (x['release_time'], x['id'])
        )

        self.events = []
        self.compute_speeds = [node['compute_speed']
                               for node in self.state]
        self.available = [
            node for node in self.state
            if node['state'] == 'active' and node['job_id'] is None and node['reserved'] == False
        ]
        self.inactive = [
            node for node in self.state
            if node['state'] == 'sleeping' and node['reserved'] == False
        ]
        self.allocated = []
        self.scheduled = []
        self.reserved_ids = []
        for job in scheduled_queue:
            self.reserved_ids.extend(job['nodes'])
        self.available = [
            node for node in self.available if node['id'] not in self.reserved_ids
        ]