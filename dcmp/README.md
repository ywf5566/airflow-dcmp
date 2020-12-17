# Airflow DAG Creation Manager Plugin

### Description
A plugin for [Apache Airflow](https://github.com/apache/incubator-airflow) that create and manage your DAG with web UI.

The plugin also provide other custom features for your DAG.

Caution:  You can only list and edit your DAG created by DAG Creation Manager.


### System Requirements

+ Airflow Versions
    + 1.X

### Deployment Instructions

1. Create the plugins folder if it doesn't exist. 
    + The location you should put it is usually at {AIRFLOW_HOME}/plugins. The specific location can be found in your airflow.cfg file:

        plugins_folder = /home/{USER_NAME}/airflow/plugins

2. git clone the code
    + ULR to Download From:
        http://www.seek-data.com/gitlab/dev/kd-airflow-dag-creation-manager-plugin.git

3. Append the following content to the end of the {AIRFLOW_HOME}/airflow.cfg file to give you control over execution:

        [dag_creation_manager]

        # see https://github.com/d3/d3-3.x-api-reference/blob/master/SVG-Shapes.md#line_interpolate
        # DEFAULT: basis
        dag_creation_manager_line_interpolate = basis
        
        # Choices for queue and pool
        dag_creation_manager_queue_pool = default_pool:default_pool|default_pool
        
        # MR queue for queue pool
        dag_creation_manager_queue_pool_mr_queue = default_pool:default_pool|default_pool
        
        # Category for display
        dag_creation_manager_category = custom
        
        # Task category for display
        dag_creation_manager_task_category = custom_task:#ffba40
        
        # Your email address to receive email
        # DEFAULT: 
        dag_creation_manager_default_email = example@email.com
        
        #dag_creation_manager_need_approver = False
        
        #dag_creation_manager_can_approve_self = True

5. (Optional) Use dag creation manager password auth
    + Note: This is not required, You can use your own auth backends
      a. Under the [webserver] section, change the following configs:

            authenticate = True

            auth_backend = dcmp.auth.backends.password_auth

6. Upgrade database

        python {AIRFLOW_PLUGINS_FOLDER}/dcmp/tools/upgradedb.py

7. Restart the Airflow Web Server

### Using DAG Creation Manager

Once you deploy the plugin and restart the web server, you can start to use the DAG Creation Manager. When you reload the Airflow Web Server page, you will see a link under the Admin tab called "DAG Creation Manager". Clicking on the link will navigate you to the following URL:

http://{AIRFLOW_HOST}:{AIRFLOW_PORT}/admin/dagcreationmanager/list

Now, you can create and manage your DAG in this web page. Enjoy!