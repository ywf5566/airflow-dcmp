# encoding: utf-8

import logging
import shutil
import re
import os
import json
import io
from time import sleep
# 防止数据在拼接的过程中由于后面的操作影响前面的已经拼接好的数据
from copy import deepcopy
from datetime import datetime
from tempfile import mkdtemp
from collections import OrderedDict

from croniter import croniter
from airflow import configuration
from airflow.utils.db import provide_session
from airflow.models import TaskInstance

from dcmp import settings as dcmp_settings
from dcmp.models import DcmpDag
from dcmp.utils import create_dagbag_by_dag_code

get_list = lambda x: x if x else []
get_string = lambda x: x.strip() if x else ""
get_int = lambda x: int(x) if x else 0
get_bool_code_true = lambda x: True if x is not False else False
get_bool_code_false = lambda x: False if x is not True else True


# 加载dag_code模板文件，返回read的文件内容
def load_dag_template(template_name):
    logging.info("loading dag template: %s" % template_name)
    with open(os.path.join(dcmp_settings.DAG_CREATION_MANAGER_DAG_TEMPLATES_DIR, template_name + ".template"),
              "r") as f:
        res = f.read()
    return res


class DAGConverter(object):
    # 元组
    DAG_ITEMS = (("dag_name", get_string, True), ("cron", get_string, True), ("start_date", get_string, False), ("owner", get_string, False))
    TASK_ITEMS = (("task_name", get_string, True), ("task_type", get_string, True), ("command", get_string, False),
                  ("upstreams", get_list, False), ("SSH_conn_id", get_string, False), ("trigger_rule", get_string, False))

    DAG_CODE_TEMPLATE = load_dag_template("dag_code")
    NONE_CRON_DAG_TEMPLATE = load_dag_template("none_cron_dag")

    BASH_TASK_CODE_TEMPLATE = r"""{0[0]} = BashOperator(
    task_id='{0[1]}',bash_command=r'''{0[2]} ''',trigger_rule='{0[3]}',dag=dag)
    """
    TRIGGER_DAG_TASK_CODE_TEMPLATE = r"""{0[0]} = TriggerDagRunOperator(
    task_id='{0[1]}',trigger_dag_id='{0[2]}',trigger_rule='{0[3]}',dag=dag)
    """
    SSH_TASK_CODE_TEMPLATE = r"""{0[0]} = SSHOperator(
    task_id='{0[1]}', ssh_conn_id='{0[2]}', command=r'''{0[3]}''',trigger_rule='{0[4]}',dag=dag)
    """
    STREAM_CODE_TEMPLATE = """%(upstream_name)s >> %(task_name)s"""

    TASK_TYPE_TO_TEMPLATE = {
        "Bash": BASH_TASK_CODE_TEMPLATE,
        "TriggerDagRun": TRIGGER_DAG_TASK_CODE_TEMPLATE,
        "SSH": SSH_TASK_CODE_TEMPLATE
    }

    JOB_NAME_RE = re.compile(r"^[a-zA-Z][a-zA-Z0-9_]+$")

    def check_job_name(self, job_name):
        return bool(self.JOB_NAME_RE.match(job_name))

    # task_dict是前端传入的task json字典,解析出task的key、value字典
    def clean_task_dict(self, task_dict, strict=False):
        # 检查task名字有无和是否合法
        task_name = task_dict.get("task_name")
        if not task_name:
            raise ValueError("task name required")
        task_name = get_string(task_name)
        if not self.check_job_name(task_name):
            raise ValueError("task %s name invalid" % task_name)

        task_res = {}  # 存放task任务
        # 遍历task元组，task-name和task-type为必须填写的
        for key, trans_func, required in self.TASK_ITEMS:
            value = task_dict.get(key)
            if required and not value:
                raise ValueError("task %s params %s required" % (task_name, key))
            if strict and key in ["queue_pool"] and not value:
                raise ValueError("task %s params %s required" % (task_name, key))
            # value = trans_func(value)
            task_res[key] = value
        return task_res

    # update--1.1 把前端dict 转化成json
    def dict_to_json(self, dag_dict, strict=False):
        if not dag_dict or not isinstance(dag_dict, dict):
            raise ValueError("dags required")

        task_dicts = dag_dict.get("tasks", [])
        if not task_dicts or not isinstance(task_dicts, list):
            raise ValueError("tasks required")
        # 循环获取dag的配置
        dag_res = {}
        for key, trans_func, required in self.DAG_ITEMS:
            value = dag_dict.get(key)
            if required and not value:
                raise ValueError("dag params %s required" % key)
            value = trans_func(value)
            dag_res[key] = value

        dag_name = dag_res["dag_name"]
        if not self.check_job_name(dag_name):
            raise ValueError("dag name invalid")

        cron = dag_res["cron"]
        if cron == "None":
            pass
        else:
            try:
                croniter(cron)
            except Exception as e:
                raise ValueError("dag params cron invalid")

        task_names = []
        tasks_res = []
        # 循环获取task的配置
        for task_dict in task_dicts:
            task_res = self.clean_task_dict(task_dict, strict=strict)
            task_name = task_res["task_name"]
            if task_name in task_names:
                raise ValueError("task %s name duplicated" % task_name)
            task_names.append(task_res["task_name"])
            tasks_res.append(task_res)

        for task_res in tasks_res:
            for upstream in task_res["upstreams"]:
                if upstream not in task_names or upstream == task_res["task_name"]:
                    raise ValueError("task %s upstream %s invalid" % (task_res["task_name"], upstream))

        dag_res["tasks"] = tasks_res
        return dag_res

    ''' 渲染配置，创建dag需要'''

    def render_confs(self, confs):
        confs = deepcopy(confs)
        # 初始化空的dag——code
        dag_codes = []
        # 根据配置创建dag
        for dag_name, conf in confs.items():
            new_conf = {}
            # ---------necessary----------
            if not conf.get("owner"):
                new_conf["owner"] = "airflow"
            else:
                new_conf["owner"] = conf["owner"]
            new_conf["dag_name"] = conf["dag_name"]
            new_conf["start_date_code"] = conf['start_date']
            cron = conf["cron"]
            if cron == "None":
                dag_code = self.NONE_CRON_DAG_TEMPLATE % new_conf
            else:
                new_conf["cron_code"] = cron
                dag_code = self.DAG_CODE_TEMPLATE % new_conf
            task_codes = []
            stream_codes = []
            for task in conf["tasks"]:
                new_task_list = [task["task_name"], task["task_name"]]
                if task["task_type"] in ["SSH"]:
                    new_task_list.append(task["SSH_conn_id"])
                new_task_list.append(task['command'])

                if not task.get("trigger_rule"):
                    new_task_list.append("all_success")
                else:
                    if task.get("trigger_rule") == 'fail':
                        new_task_list.append("one_failed")
                    else:
                        new_task_list.append("all_success")

                task_template = self.TASK_TYPE_TO_TEMPLATE.get(task['task_type'])
                if task_template:
                    task_codes.append(task_template.format(new_task_list))
                else:
                    continue
                for upstream in task["upstreams"]:
                    stream_code = self.STREAM_CODE_TEMPLATE % {
                        "task_name": task["task_name"],
                        "upstream_name": upstream,
                    }
                    stream_codes.append(stream_code)
            dag_code = "%s\n%s\n%s" % (dag_code, "\n".join(task_codes), "\n".join(stream_codes))
            # -----------necessary--------------
            dag_codes.append((dag_name, dag_code))
        return dag_codes

    @provide_session
    def refresh_dags(self, session=None):
        confs = OrderedDict()
        dcmp_dags = session.query(DcmpDag).all()
        for dcmp_dag in dcmp_dags:
            conf = dcmp_dag.get_approved_conf(session=session)
            if conf:
                confs[dcmp_dag.dag_name] = conf

        dag_codes = self.render_confs(confs)

        tmp_dir = mkdtemp(prefix="dcmp_deployed_dags_")
        os.chmod(tmp_dir, 0o755)
        for dag_name, dag_code in dag_codes:
            with open(os.path.join(tmp_dir, dag_name + ".py"), "w") as f:
                f.write(dag_code)

        err = None
        for _ in range(3):
            shutil.rmtree(dcmp_settings.DAG_CREATION_MANAGER_DEPLOYED_DAGS_FOLDER, ignore_errors=True)
            try:
                shutil.copytree(tmp_dir, dcmp_settings.DAG_CREATION_MANAGER_DEPLOYED_DAGS_FOLDER)
            except Exception as e:
                err = e
                sleep(1)
            else:
                shutil.rmtree(tmp_dir, ignore_errors=True)
                break
        else:
            shutil.rmtree(tmp_dir, ignore_errors=True)
            raise err

    # update--1.2 根据配置创建dag
    def create_dagbag_by_conf(self, conf):

        _, dag_code = self.render_confs({conf["dag_name"]: conf})[0]
        # 在utils包中
        return create_dagbag_by_dag_code(dag_code)

    # update的第一步
    def clean_dag_dict(self, dag_dict, strict=False):
        conf = self.dict_to_json(dag_dict, strict=strict)
        dagbag = self.create_dagbag_by_conf(conf)
        if dagbag.import_errors:
            raise ImportError(dagbag.import_errors.items()[0][1])
        return conf

    def create_dag_by_conf(self, conf):
        return self.create_dagbag_by_conf(conf).dags[conf["dag_name"]]

    def create_task_by_task_conf(self, task_conf, dag_conf=None):
        if not task_conf.get("task_name"):
            task_conf["task_name"] = "tmp_task"
        task_conf["upstreams"] = []
        if not dag_conf:
            dag_conf = {
                "dag_name": "tmp_dag",
                "cron": "0 * * * *",
                "category": "default",
            }
        dag_conf["tasks"] = [task_conf]
        conf = self.dict_to_json(dag_conf)
        dag = self.create_dag_by_conf(conf)
        task = dag.get_task(task_id=task_conf["task_name"])
        if not task:
            raise ValueError("invalid conf")
        return task

    def create_task_instance_by_task_conf(self, task_conf, dag_conf=None, execution_date=None):
        if execution_date is None:
            execution_date = datetime.now()
        task = self.create_task_by_task_conf(task_conf, dag_conf=dag_conf)
        ti = TaskInstance(task, execution_date)
        return ti

    def render_task_conf(self, task_conf, dag_conf=None, execution_date=None):
        ti = self.create_task_instance_by_task_conf(task_conf, dag_conf=dag_conf, execution_date=execution_date)
        ti.render_templates()
        res = OrderedDict()
        for template_field in ti.task.__class__.template_fields:
            res[template_field] = getattr(ti.task, template_field)
        return res


dag_converter = DAGConverter()
