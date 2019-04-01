import time

from cbas.cbas_base import CBASBaseTest
from remote.remote_util import RemoteMachineShellConnection
from testconstants import WIN_COUCHBASE_LOGS_PATH, LINUX_COUCHBASE_LOGS_PATH
from lib.memcached.helper.data_helper import MemcachedClientHelper
from couchbase_helper.tuq_generators import JsonGenerator


class CBASScanConsistency(CBASBaseTest):
    
    @staticmethod
    def fetch_log_path(shell, log_file_name='analytics_info.log'):
        os = shell.return_os_type()
        path = None
        if os == 'linux':
            path = LINUX_COUCHBASE_LOGS_PATH + "/" + log_file_name
        elif os == 'windows':
            path = WIN_COUCHBASE_LOGS_PATH + "/" + log_file_name
        else:
            raise ValueError('Path unknown for os type {0}'.format(os))
        return path

    def setUp(self):
        super(CBASScanConsistency, self).setUp()
        
        self.log.info('Fetch scan consistency parameters')
        self.scan_consistency = self.input.param('scan_consistency', None)
        self.scan_wait = self.input.param('scan_wait', None)
    
    def test_scan_consistency_parameters(self):
        
        self.log.info('Execute SQL++ query with all scan_wait parameters')
        query = "select 1"
        scan_waits = ['10ns', '10us', '10ms', '5s', '1m', '1h', '0.1m']
        for wait in scan_waits:
            response, _, _, results, _ = self.cbas_util.execute_statement_on_cbas_util(query, scan_wait=wait)
            self.assertEqual(response, "success", "Query failed")
            self.assertEqual(results[0]['$1'], 1, msg="Query result mismatch")
        
        self.log.info('Execute SQL++ query with incorrect scan_consistency parameters')
        query = "select 1"
        response, _, error, _, _ = self.cbas_util.execute_statement_on_cbas_util(query, scan_consistency='at_plus')
        self.assertEqual(response, "fatal", "Query must fail as scan consistency parameter is not supported")
        self.assertEqual(error[0]['msg'], 'Unsupported scan consistency (at_plus)', msg='Error message mismatch')
        self.assertEqual(error[0]['code'], 21006, msg='Error code mismatch')
        
        self.log.info('Execute SQL++ query with incorrect scan_wait parameters')
        response, _, error, _, _ = self.cbas_util.execute_statement_on_cbas_util(query, scan_wait='1Y')
        self.assertEqual(response, "fatal", "Query must fail as scan consistency parameter is not supported")
        self.assertEqual(error[0]['msg'], 'Unknown duration unit Y', msg='Error message mismatch')
        self.assertEqual(error[0]['code'], 21001, msg='Error code mismatch')
        
        self.log.info('Execute SQL++ query with incorrect scan_wait unit parameters')
        response, _, error, _, _ = self.cbas_util.execute_statement_on_cbas_util(query, scan_wait='1')
        self.assertEqual(response, "fatal", "Query must fail as scan consistency parameter is not supported")
        self.assertEqual(error[0]['msg'], 'Invalid duration "1"', msg='Error message mismatch')
        self.assertEqual(error[0]['code'], 21000, msg='Error code mismatch')
        
        self.log.info('Load documents in the default bucket')
        self.perform_doc_ops_in_all_cb_buckets(self.num_items, "create", 0, self.num_items)
        
        self.log.info('Create dataset')
        self.cbas_util.create_dataset_on_bucket(self.cb_bucket_name, self.cbas_dataset_name)
        
        self.log.info('Connect link')
        self.cbas_util.connect_link()
        
        self.log.info('Verify dataset count')
        self.cbas_util.validate_cbas_dataset_items_count(self.cbas_dataset_name, self.num_items)
        
        self.log.info('Execute SQL++ query with scan_wait parameter that results in timeout')
        query = 'select * from %s' % self.cbas_dataset_name
        response, _, error, _, _ = self.cbas_util.execute_statement_on_cbas_util(query, scan_consistency='request_plus', scan_wait='1ns')
        self.assertEqual(response, "fatal", "Query must fail as scan wait time specified is very low")
        self.assertEqual(error[0]['msg'], 'Scan wait timeout', msg='Error message mismatch')
        self.assertEqual(error[0]['code'], 23028, msg='Error code mismatch')
        
        self.log.info('Disconnect link')
        self.cbas_util.disconnect_link()
        
        self.log.info('Execute SQL++ query with link disconnected')
        response, _, error, _, _ = self.cbas_util.execute_statement_on_cbas_util(query, scan_consistency='request_plus', scan_wait='1ns')
        self.assertEqual(response, "fatal", "Query must fail as KV bucket is disconnected")
        self.assertEqual(error[0]['msg'], 'Bucket default on link Local in dataverse Default is not connected', msg='Error message mismatch')
        self.assertEqual(error[0]['code'], 23027, msg='Error code mismatch')

    def test_scan_consistency_parameters_are_logged(self):

        self.log.info('Execute SQL++ query with scan_consistency and scan_wait')
        query = "select 1"
        response, _, _, results, _ = self.cbas_util.execute_statement_on_cbas_util(query, scan_consistency=self.scan_consistency, scan_wait=self.scan_wait)
        self.assertEqual(response, "success", "Query %s failed. Actual: %s, Expected:%s" % (query, response, 'success'))
        
        self.log.info('Verify query result')
        self.assertEqual(results[0]['$1'], 1, msg="Query result mismatch")

        self.log.info('Verify scan consistency parameter is logged')
        shell = RemoteMachineShellConnection(self.cbas_node)
        path = CBASScanConsistency.fetch_log_path(shell)
        cmd = "grep '\"%s\":\"%s\"' %s | tail -1" % ('scanConsistency', self.scan_consistency, path)
        result, _ = shell.execute_command(cmd)
        self.assertTrue('"scanConsistency":"%s"' % self.scan_consistency in ''.join(result), msg="'scanConsistency' not logged")
        
        self.log.info('Verify scan wait parameter is logged')
        cmd = "grep '\"%s\":\"%s\"' %s | tail -1" % ('scanWait', self.scan_wait, path)
        result, _ = shell.execute_command(cmd)
        self.assertTrue('"scanWait":"%s"' % self.scan_wait in ''.join(result), msg="'scanWait' not logged")
        shell.disconnect()
    
    def test_scan_consistency_post_memcached_crash(self):
        
        self.log.info('Load documents in the default bucket')
        self.perform_doc_ops_in_all_cb_buckets(self.num_items, "create", 0, self.num_items)
        
        self.log.info('Create dataset')
        self.cbas_util.create_dataset_on_bucket(self.cb_bucket_name, self.cbas_dataset_name)
        
        self.log.info('Connect link')
        self.cbas_util.connect_link()
        
        self.log.info('Verify dataset count')
        self.cbas_util.validate_cbas_dataset_items_count(self.cbas_dataset_name, self.num_items)
        
        self.log.info('Stopping persistence on KV')
        mem_client = MemcachedClientHelper.direct_client(self.input.servers[0], self.cb_bucket_name)
        mem_client.stop_persistence()
        
        self.log.info('Performing Mutations')
        self.perform_doc_ops_in_all_cb_buckets(self.num_items, "create", self.num_items, self.num_items * 2)
        
        self.log.info('Kill Memcached process')
        shell = RemoteMachineShellConnection(self.master)
        shell.kill_memcached()
        
        self.log.info('Validate count')
        query = 'select count(*) from %s' % self.cbas_dataset_name
        dataset_count=0
        start_time = time.time()
        output = []
        while time.time() < start_time + 120:
            try:
                response, _, _, results, _ = self.cbas_util.execute_statement_on_cbas_util(query, scan_consistency=self.scan_consistency, scan_wait=self.scan_wait)
                self.assertEqual(response, "success", "Query failed...")
                dataset_count = results[0]['$1']
                if dataset_count == self.num_items:
                    break
            except Exception as e:
                self.log.info('Try again as memcached might be recovering...')
        
        self.log.info('Verify dataset count is equal to number of items in KV')
        count_n1ql = self.rest.query_tool('select count(*) from %s' % self.cb_bucket_name)['results'][0]['$1']
        self.assertEqual(dataset_count, count_n1ql, msg='KV-CBAS count mismatch. Actual %s, expected %s' % (dataset_count, count_n1ql))
    
    def test_scan_consistency_post_analytics_failover(self):
        
        self.log.info('Load documents in the default bucket')
        self.perform_doc_ops_in_all_cb_buckets(self.num_items, "create", 0, self.num_items)
        
        self.log.info('Add a cbas node')
        self.assertTrue(self.add_node(self.cbas_servers[0], services=["cbas"], rebalance=True), msg="Failed to add CBAS node")
        
        self.log.info('Create dataset')
        self.cbas_util.create_dataset_on_bucket(self.cb_bucket_name, self.cbas_dataset_name)
        
        self.log.info('Connect link')
        self.cbas_util.connect_link()
        
        self.log.info('Verify dataset count')
        self.cbas_util.validate_cbas_dataset_items_count(self.cbas_dataset_name, self.num_items)
        
        self.log.info('fail-over the node')
        fail_task = self._cb_cluster.async_failover(self.input.servers, [self.cbas_servers[0]], False)
        fail_task.get_result()
        
        self.log.info('Rebalance to remove failover node')
        self.rebalance(wait_for_completion=False)
        
        self.log.info('Validate count post failover rebalance out')
        dataset_count = 0
        start_time = time.time()
        while time.time() < start_time + 120:
            try:
                query = 'select count(*) from %s' % self.cbas_dataset_name
                response, _, _, results, _ = self.cbas_util.execute_statement_on_cbas_util(query, scan_consistency=self.scan_consistency, scan_wait=self.scan_wait)
                self.assertEqual(response, "success", "Query failed...")
                dataset_count = results[0]['$1']
                break
            except Exception as e:
                self.log.info('Try again as rebalance might be in progress or Analytics might be recovering...')
        
        self.log.info('Verify dataset count is equal to number of items in KV')
        count_n1ql = self.rest.query_tool('select count(*) from %s' % self.cb_bucket_name)['results'][0]['$1']
        self.assertEqual(dataset_count, count_n1ql, msg='KV-CBAS count mismatch. Actual %s, expected %s' % (dataset_count, count_n1ql))
    
    def test_scan_consistency_with_kv_mutations(self):
        
        self.log.info('Load documents in the default bucket')
        self.perform_doc_ops_in_all_cb_buckets(self.num_items, "create", 0, self.num_items)
        
        self.log.info('Create dataset')
        self.cbas_util.create_dataset_on_bucket(self.cb_bucket_name, self.cbas_dataset_name)
        
        self.log.info('Connect link')
        self.cbas_util.connect_link()
        
        self.log.info('Verify dataset count')
        self.cbas_util.validate_cbas_dataset_items_count(self.cbas_dataset_name, self.num_items)
        
        self.log.info("Perform async doc operations on KV")
        json_generator = JsonGenerator()
        generators = json_generator.generate_docs_simple(docs_per_day=self.num_items * 4, start=self.num_items)
        kv_task = self._async_load_all_buckets(self.master, generators, "create", 0, batch_size=5000)
        
        self.log.info('Validate count')
        query = 'select count(*) from %s' % self.cbas_dataset_name
        dataset_count=0
        start_time = time.time()
        output_with_scan = []
        output_without_scan = []
        while time.time() < start_time + 120:
            try:
                response_with_scan, _, _, results_with_scan, _ = self.cbas_util.execute_statement_on_cbas_util(query, scan_consistency=self.scan_consistency, scan_wait=self.scan_wait)
                self.assertEqual(response_with_scan, "success", "Query failed...")
                output_with_scan.append(results_with_scan[0]['$1'])
                
                response_without_scan, _, _, results_without_scan, _ = self.cbas_util.execute_statement_on_cbas_util(query, scan_consistency='not_bounded')
                self.assertEqual(response_without_scan, "success", "Query failed...")
                output_without_scan.append(results_without_scan[0]['$1'])
                
                if results_without_scan[0]['$1'] == self.num_items * 4:
                    break
            except Exception as e:
                self.log.info('Try again neglect failures...')
        
        self.log.info("Get KV ops result")
        for task in kv_task:
            task.get_result()
        
        self.log.info('Compare the output result length of count query with scan and with scan parameters')
        self.assertTrue(len(set(output_with_scan)) < len(set(output_without_scan)), msg='Select query with scan consistency must take fewer results')
        cbas_datasets = sorted(list(set(output_with_scan)))
        count_n1ql = self.rest.query_tool('select count(*) from %s' % self.cb_bucket_name)['results'][0]['$1']
        self.assertEqual(cbas_datasets[len(cbas_datasets)-1], count_n1ql, msg='KV-CBAS count mismatch. Actual %s, expected %s' % (dataset_count, count_n1ql))
    
    def test_scan_consistency_post_kv_documents_load(self):
        
        self.log.info('Load documents in the default bucket')
        self.perform_doc_ops_in_all_cb_buckets(self.num_items, "create", 0, self.num_items)
        
        self.log.info('Create dataset')
        self.cbas_util.create_dataset_on_bucket(self.cb_bucket_name, self.cbas_dataset_name)
        
        self.log.info('Connect link')
        self.cbas_util.connect_link()
        
        self.log.info('Validate count')
        query = 'select count(*) from %s' % self.cbas_dataset_name
        response, _, _, results, _ = self.cbas_util.execute_statement_on_cbas_util(query, scan_consistency=self.scan_consistency, scan_wait=self.scan_wait)
        self.assertEqual(response, "success", "Query failed...")
        dataset_count = results[0]['$1']
        count_n1ql = self.rest.query_tool('select count(*) from %s' % self.cb_bucket_name)['results'][0]['$1']
        self.assertEqual(dataset_count, count_n1ql, msg='KV-CBAS count mismatch. Actual %s, expected %s' % (dataset_count, count_n1ql))
    
    def test_scan_consistency_post_kv_bucket_flush(self):
        
        self.log.info('Load documents in the default bucket')
        self.perform_doc_ops_in_all_cb_buckets(self.num_items, "create", 0, self.num_items)
        
        self.log.info('Create dataset')
        self.cbas_util.create_dataset_on_bucket(self.cb_bucket_name, self.cbas_dataset_name)
        
        self.log.info('Connect link')
        self.cbas_util.connect_link()
        
        self.log.info('Verify dataset count')
        self.cbas_util.validate_cbas_dataset_items_count(self.cbas_dataset_name, self.num_items)
        
        self.log.info('Flush KV bucket')
        self.cluster.bucket_flush(server=self.master, bucket=self.cb_bucket_name)
        
        self.log.info('Validate count')
        count_n1ql = self.rest.query_tool('select count(*) from %s' % self.cb_bucket_name)['results'][0]['$1']
        query = 'select count(*) from %s' % self.cbas_dataset_name
        dataset_count = []
        start_time = time.time()
        while time.time() < start_time + 120:
            try:
                response, _, _, results, _ = self.cbas_util.execute_statement_on_cbas_util(query, scan_consistency=self.scan_consistency, scan_wait=self.scan_wait)
                self.assertEqual(response, "success", "Query failed...")
                dataset_count.append(results[0]['$1'])
                if results[0]['$1'] == 0:
                    break
            except Exception as e:
                self.log.info('Neglect failures related to bucket disconnect...')
        dataset_count = sorted(list(set(dataset_count)))
        if len(dataset_count) > 1:
            self.assertEqual(len(dataset_count), 2, msg='In case of full rollback the dataset count must reduce from %s to 0' % self.num_items)
        self.assertEqual(dataset_count[0], 0, msg='KV-CBAS count mismatch. Actual %s, expected %s' % (dataset_count, 0))
    
    def test_kv_topology_change_does_not_impact_scan_consistency(self):
        
        self.log.info('Load documents in the default bucket')
        self.perform_doc_ops_in_all_cb_buckets(self.num_items, "create", 0, self.num_items)
        
        self.log.info('Create dataset')
        self.cbas_util.create_dataset_on_bucket(self.cb_bucket_name, self.cbas_dataset_name)
        
        self.log.info('Connect link')
        self.cbas_util.connect_link()
        
        self.log.info('Add an extra KV node')
        self.add_node(self.servers[1], rebalance=True, wait_for_rebalance_completion=False)
        
        self.log.info('Validate count')
        query = 'select count(*) from %s' % self.cbas_dataset_name
        response, _, _, results, _ = self.cbas_util.execute_statement_on_cbas_util(query, scan_consistency=self.scan_consistency, scan_wait=self.scan_wait)
        self.assertEqual(response, "success", "Query failed...")
        dataset_count = results[0]['$1']
        count_n1ql = self.rest.query_tool('select count(*) from %s' % self.cb_bucket_name)['results'][0]['$1']
        self.assertEqual(dataset_count, count_n1ql, msg='KV-CBAS count mismatch. Actual %s, expected %s' % (dataset_count, count_n1ql))
    
    def test_analytics_topology_change_does_not_impact_scan_consistency(self):
        
        self.log.info('Load documents in the default bucket')
        self.perform_doc_ops_in_all_cb_buckets(self.num_items, "create", 0, self.num_items)
        
        self.log.info('Create dataset')
        self.cbas_util.create_dataset_on_bucket(self.cb_bucket_name, self.cbas_dataset_name)
        
        self.log.info('Connect link')
        self.cbas_util.connect_link()
        
        self.log.info('Add an extra KV node')
        self.add_node(self.cbas_servers[0], rebalance=True, wait_for_rebalance_completion=False)
        
        self.log.info('Validate count')
        query = 'select count(*) from %s' % self.cbas_dataset_name
        count_n1ql = self.rest.query_tool('select count(*) from %s' % self.cb_bucket_name)['results'][0]['$1']
        start_time = time.time()
        dataset_count = 0
        while time.time() < start_time + 300:
            try:
                response, _, _, results, _ = self.cbas_util.execute_statement_on_cbas_util(query, scan_consistency=self.scan_consistency)
                self.assertEqual(response, "success", "Query failed...")
                dataset_count = results[0]['$1']
                break
            except Exception as e:
                self.log.info('Neglect analytics server recovery errors...')
        self.assertEqual(dataset_count, count_n1ql, msg='KV-CBAS count mismatch. Actual %s, expected %s' % (dataset_count, count_n1ql))
