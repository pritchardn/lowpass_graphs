"""
Runs all workflows with 10 random trials
@author Nicholas Pritchard
"""
from pathlib import Path
import os
import glob
import json
import optparse

from dlg.deploy import common
from dlg.common.reproducibility.reproducibility import init_lgt_repro_data, init_lg_repro_data, \
    init_pgt_unroll_repro_data, init_pgt_partition_repro_data, init_pg_repro_data
from dlg.common.reproducibility.constants import ReproducibilityFlags
from dlg.dropmake.pg_generator import unroll, partition, resource_map
from dlg.translator.tool_commands import submit
import dlg.common.reproducibility.reprodata_compare

import shutil

TRIAL_COUNT = 10
RMODE = str(ReproducibilityFlags.ALL.value)


def adjust_workflow_seed(workflow_loc, seed):
    with open(workflow_loc, 'r') as infile:
        wflow = json.load(infile)
    for drop in wflow['nodeDataArray']:
        if drop['category'] == "PythonApp" and drop["text"] == "LPAddNoise":
            for application_arg in drop['applicationArgs']:
                if application_arg['name'] == 'randomseed':
                    application_arg['value'] = seed
    return wflow


def run_workflow(workflow):
    parser = optparse.OptionParser()
    (opts, args) = parser.parse_args()

    lg = init_lg_repro_data(init_lgt_repro_data(workflow, RMODE))
    pgt = unroll(lg)
    init_pgt_unroll_repro_data(pgt)
    repro = pgt.pop()
    pgt = partition(pgt, "metis")
    pgt.append(repro)
    pgt = init_pgt_partition_repro_data(pgt)

    nodes = ["127.0.0.1", "127.0.0.1"]

    repro = pgt.pop()
    pg = resource_map(
        pgt, nodes
    )
    pg.append(repro)
    init_pg_repro_data(pg)
    session_id = common.submit(pg)
    common.monitor_sessions(
        session_id
    )
    return session_id


def main(workflow_loc: Path, result_loc: Path, output_loc: Path):
    # Get list of workflows to run
    workflows = glob.glob(str(workflow_loc.joinpath("*.graph")))
    reprodata_files = []
    print(workflows)
    submit_count = 0
    for workflow in workflows:
        print(workflow)
        workflow_name = workflow.split(".")[0].split(os.sep)[-1]
        for i in range(TRIAL_COUNT):
            logical_graph = adjust_workflow_seed(workflow, i)
            session_id = run_workflow(logical_graph)
            submit_count += 1
            # Collate reprodata outputs.
            current_output_file = result_loc.joinpath(session_id, 'reprodata.out')
            temp_output_location = output_loc.joinpath(workflow_name, f"{workflow_name}-{str(i)}")
            os.makedirs(temp_output_location, exist_ok=True)
            reprodata_files.append(temp_output_location.absolute())
            shutil.move(current_output_file, temp_output_location, copy_function=shutil.copy)
    print(submit_count)
    # Perform comparison
    dlg.common.reproducibility.reprodata_compare._main(reprodata_files, output_loc)


if __name__ == "__main__":
    workflow_location = Path("./").absolute()
    results_location = Path(os.path.expanduser("~/dlg/logs")).absolute()
    output_location = Path("./results/").absolute()
    os.makedirs(output_location, exist_ok=True)
    main(workflow_location, results_location, output_location)
