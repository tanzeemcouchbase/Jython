import datetime

from basetestcase import RemoteMachineShellConnection
from bucket_utils.bucket_ready_functions import bucket_utils
from cbas.cbas_base import CBASBaseTest
from membase.helper.rebalance_helper import RebalanceHelper
from couchbase_helper.documentgenerator import DocumentGenerator


class CBASBugAutomation(CBASBaseTest):

    def setUp(self):
        # Invoke CBAS setUp method
        super(CBASBugAutomation, self).setUp()  

    @staticmethod
    def generate_documents(start_at, end_at):
        age = range(70)
        first = ['james', 'sharon', 'dave', 'bill', 'mike', 'steve']
        profession = ['doctor', 'lawyer']
        template = '{{ "number": {0}, "first_name": "{1}" , "profession":"{2}", "mutated":0}}'
        documents = DocumentGenerator('test_docs', template, age, first, profession, start=start_at, end=end_at)
        return documents

    def test_multiple_cbas_data_set_creation(self):

        '''
        -i b/resources/4-nodes-template.ini -t cbas.cbas_bug_automation.CBASBugAutomation.test_multiple_cbas_data_set_creation,default_bucket=False,
        sample_bucket_docs_count=31591,cb_bucket_name=travel-sample,num_of_datasets=8,where_field=country,where_value=United%States:France:United%Kingdom
        '''

        self.log.info("Load sample bucket")
        couchbase_bucket_docs_count = self.input.param("sample_bucket_docs_count", self.travel_sample_docs_count)
        couchbase_bucket_name = self.input.param("cb_bucket_name", self.cb_bucket_name)
        result = self.load_sample_buckets(servers=list([self.master]),
                                          bucketName=couchbase_bucket_name,
                                          total_items=couchbase_bucket_docs_count)
        self.assertTrue(result, "Failed to load sample bucket")

        self.log.info("Create connection")
        self.cbas_util.createConn(couchbase_bucket_name)

        self.log.info("Create a CBAS bucket")
        self.cbas_util.create_bucket_on_cbas(cbas_bucket_name=self.cbas_bucket_name, # default value for cbas_bucket_name is configured inside the cbas_base
                                             cb_bucket_name=couchbase_bucket_name)
        
        '''
        If where value contains space replace it with %, inside test replace the % with space. 
        Also, if we have more than 1 value delimit them using :
        '''
        field = self.input.param("where_field", "")
        values = self.input.param("where_value", "")
        if values:
            values = values.replace("%", " ").split(":")

        self.log.info("Create data-sets")
        num_of_datasets = self.input.param("num_of_datasets", 8)
        for index in range(num_of_datasets):
            if index < len(values) and field and values:
                self.cbas_util.create_dataset_on_bucket(cbas_bucket_name=self.cbas_bucket_name,
                                                        cbas_dataset_name=self.cbas_dataset_name + str(index),
                                                        where_field=field, where_value=values[index])
            else:
                self.cbas_util.create_dataset_on_bucket(cbas_bucket_name=self.cbas_bucket_name,
                                                        cbas_dataset_name=self.cbas_dataset_name + str(index))

        self.log.info("Connect to CBAS bucket")
        self.cbas_util.connect_to_bucket(cbas_bucket_name=self.cbas_bucket_name,
                                         cb_bucket_password=self.cb_bucket_password)

        self.log.info("Wait for ingestion to completed and assert count")
        for index in range(num_of_datasets):
            if index < len(values) and field and values:
                count_n1ql = self.rest.query_tool('select count(*) from `%s` where %s = "%s"' % (self.cb_bucket_name, field, values[index]))['results'][0]['$1']
            else:
                count_n1ql = self.rest.query_tool('select count(*) from `%s`' % self.cb_bucket_name)['results'][0]['$1']
            self.cbas_util.wait_for_ingestion_complete([self.cbas_dataset_name + str(index)], count_n1ql)
            _, _, _, results, _ = self.cbas_util.execute_statement_on_cbas_util('select count(*) from `%s`' % (self.cbas_dataset_name + str(index)))
            count_ds = results[0]["$1"]
            self.assertEqual(count_ds, count_n1ql, msg="result count mismatch between N1QL and Analytics")


    def test_data_ingestion_is_not_triggered_on_killing_cbas_java_processon_on_nc_nodes(self):

        '''
        -i b/resources/4-nodes-template.ini -t cbas.cbas_bug_automation.CBASBugAutomation.test_data_ingestion_is_not_triggered_on_killing_cbas_java_processon_on_nc_nodes,
        default_bucket=False,cb_bucket_name=beer-sample,cbas_bucket_name=cbas_beer_sample,self.cbas_dataset_name=beer_sample_dataset,process_name=cbas,service_name=/opt/couchbase/lib/cbas/runtime/bin/java,add_all_cbas_nodes=True
        '''

        self.log.info("Load beer sample bucket")
        result = self.load_sample_buckets(servers=list([self.master]),
                                          bucketName=self.cb_bucket_name,
                                          total_items=self.beer_sample_docs_count)
        self.assertTrue(result, "Failed to load beer-sample bucket")

        self.log.info("Create connection")
        self.cbas_util.createConn(self.cb_bucket_name)

        self.log.info("Create a CBAS bucket")
        self.cbas_util.create_bucket_on_cbas(cbas_bucket_name=self.cbas_bucket_name,
                                             cb_bucket_name=self.cb_bucket_name)

        self.log.info("Create data-set")
        self.cbas_util.create_dataset_on_bucket(cbas_bucket_name=self.cbas_bucket_name,
                                                cbas_dataset_name=self.cbas_dataset_name)

        self.log.info("Connect to CBAS bucket")
        self.cbas_util.connect_to_bucket(cbas_bucket_name=self.cbas_bucket_name,
                                         cb_bucket_password=self.cb_bucket_password)

        self.log.info("Wait for ingestion to complete")
        self.cbas_util.wait_for_ingestion_complete([self.cbas_dataset_name], self.beer_sample_docs_count)

        self.log.info("Validate count in CBAS bucket")
        items_in_cbas_bucket, _ = self.cbas_util.get_num_items_in_cbas_dataset(self.cbas_dataset_name)
        self.log.info("Items in cbas data set before killing process %s" % items_in_cbas_bucket)
        self.assertEqual(items_in_cbas_bucket, self.beer_sample_docs_count)

        self.log.info("Get the non-cc node to kill")
        cc_ip = self.cbas_util.retrieve_cc_ip()
        for cbas_server in self.cbas_servers:
            if cc_ip != cbas_server.ip:
                nc_cbas_node = cbas_server
                print(nc_cbas_node.ip)
                break

        if not nc_cbas_node:
            self.fail("Cluster must have more than 1 cbas node")

        self.log.info("Establish a remote connection on nc node")
        shell = RemoteMachineShellConnection(nc_cbas_node)

        self.log.info("kill non cc node with signum -15")
        process_name = self.input.param('process_name', None)
        service_name = self.input.param('service_name', None)
        shell.kill_process(process_name, service_name, signum=15)

        self.log.info("Observe no reingestion on node after restart")
        items_in_cbas_bucket, _ = self.cbas_util.get_num_items_in_cbas_dataset(self.cbas_dataset_name)
        self.log.info("Items in cbas data set after kill -15 %s" % items_in_cbas_bucket)
        self.assertEqual(items_in_cbas_bucket, self.beer_sample_docs_count)

        self.log.info("kill non cc node with signum -9")
        shell.kill_process(process_name, service_name)

        self.log.info("Observe no reingestion on node after restart")
        items_in_cbas_bucket, _ = self.cbas_util.get_num_items_in_cbas_dataset(self.cbas_dataset_name)
        self.log.info("Items in cbas data set after kill -9 %s" % items_in_cbas_bucket)
        self.assertEqual(items_in_cbas_bucket, self.beer_sample_docs_count)

    def test_cbas_queries_in_parallel_with_data_ingestion_on_multiple_cb_buckets(self):

        '''
        -i b/resources/4-nodes-template.ini -t cbas.cbas_bug_automation.CBASBugAutomation.test_cbas_queries_in_parallel_with_data_ingestion_on_multiple_cb_buckets,
        default_bucket=False,num_of_cb_buckets=4,items=1000,minutes_to_run=1
        '''

        self.log.info("Get the available memory quota")
        bucket_util = bucket_utils(self.master)
        self.info = bucket_util.rest.get_nodes_self()
        threadhold_memory = 1024
        total_memory_in_mb = self.info.memoryTotal / 1024 ** 2
        total_available_memory_in_mb = total_memory_in_mb
        active_service = self.info.services

        if "index" in active_service:
            total_available_memory_in_mb -= self.info.indexMemoryQuota
        if "fts" in active_service:
            total_available_memory_in_mb -= self.info.ftsMemoryQuota
        if "cbas" in active_service:
            total_available_memory_in_mb -= self.info.cbasMemoryQuota
        if "eventing" in active_service:
            total_available_memory_in_mb -= self.info.eventingMemoryQuota
        
        print(total_memory_in_mb)
        available_memory =  total_available_memory_in_mb - threadhold_memory 
        self.rest.set_service_memoryQuota(service='memoryQuota', memoryQuota=available_memory)

        self.log.info("Add a KV nodes")
        result = self.add_node(self.servers[1], services=["kv"], rebalance=False)
        self.assertTrue(result, msg="Failed to add KV node.")

        self.log.info("Add a CBAS nodes")
        result = self.add_node(self.cbas_servers[0], services=["cbas"], rebalance=True)
        self.assertTrue(result, msg="Failed to add CBAS node.")

        self.log.info("Create CB buckets")
        num_of_cb_buckets = self.input.param("num_of_cb_buckets", 4)
        for i in range(num_of_cb_buckets):
            self.create_bucket(self.master, "default" + str(i), bucket_ram=(available_memory / num_of_cb_buckets))

        self.log.info("Create connections for CB buckets")
        for i in range(num_of_cb_buckets):
            self.cbas_util.createConn("default" + str(i))

        self.log.info("Create CBAS buckets")
        for i in range(num_of_cb_buckets):
            self.cbas_util.create_bucket_on_cbas(cbas_bucket_name="cbas_default" + str(i),
                                                 cb_bucket_name="default" + str(i))

        self.log.info("Create data-sets")
        for i in range(num_of_cb_buckets):
            self.cbas_util.create_dataset_on_bucket(cbas_bucket_name="cbas_default" + str(i),
                                                    cbas_dataset_name="cbas_default_ds" + str(i))

        self.log.info("Connect to CBAS buckets")
        for i in range(num_of_cb_buckets):
            result = self.cbas_util.connect_to_bucket(cbas_bucket_name="cbas_default" + str(i),
                                                      cb_bucket_password=self.cb_bucket_password)
            self.assertTrue(result, msg="Failed to connect cbas bucket")

        self.log.info("Generate documents")
        num_of_documents_per_insert_update = self.input.param("items", 1000)
        load_gen = CBASBugAutomation.generate_documents(0, num_of_documents_per_insert_update)

        self.log.info("Asynchronously insert documents in CB buckets")
        tasks = self._async_load_all_buckets(server=self.master, kv_gen=load_gen, op_type="create", exp=0, batch_size=100)
        for task in tasks:
            self.log.info(task.get_result())

        self.log.info("Asynchronously create/update documents in CB buckets")
        start_insert_update_from = num_of_documents_per_insert_update
        end_insert_update_at = start_insert_update_from + num_of_documents_per_insert_update
        minutes_to_run = self.input.param("minutes_to_run", 5)
        end_time = datetime.datetime.now() + datetime.timedelta(minutes=int(minutes_to_run))
        while datetime.datetime.now() < end_time:
            try:
                self.log.info("start creation of new documents")
                load_gen = CBASBugAutomation.generate_documents(start_insert_update_from, end_insert_update_at)
                tasks = self._async_load_all_buckets(server=self.master, kv_gen=load_gen, op_type="create", exp=0, batch_size=100)
                for task in tasks:
                    self.log.info(task.get_result())

                self.log.info("start updating of documents created in the last iteration")
                load_previous_iteration_gen = CBASBugAutomation.generate_documents(start_insert_update_from - num_of_documents_per_insert_update, end_insert_update_at - num_of_documents_per_insert_update)
                tasks = self._async_load_all_buckets(server=self.master, kv_gen=load_previous_iteration_gen, op_type="update", exp=0, batch_size=100)
                for task in tasks:
                    self.log.info(task.get_result())

                start_insert_update_from = end_insert_update_at
                end_insert_update_at = end_insert_update_at + num_of_documents_per_insert_update

            except:
                pass

            for i in range(num_of_cb_buckets):
                try:
                    self.cbas_util.execute_statement_on_cbas_util('select count(*) from `%s`' % ("cbas_default_ds" + str(i)))
                    self.cbas_util.execute_statement_on_cbas_util('select * from `%s`' % ("cbas_default_ds" + str(i)))
                except Exception as e:
                    self.log.info(str(e))

        self.log.info("Assert document count in CBAS dataset")
        for i in range(num_of_cb_buckets):
            count_n1ql = self.rest.query_tool('select count(*) from `%s`' % ("default" + str(i)))['results'][0]['$1']
            result = self.cbas_util.validate_cbas_dataset_items_count(dataset_name="cbas_default_ds" + str(i), expected_count=count_n1ql, expected_mutated_count=count_n1ql-num_of_documents_per_insert_update)

    def tearDown(self):
        super(CBASBugAutomation, self).tearDown()