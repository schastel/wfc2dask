"""
TODO

See README.md
"""
from wfc2dask.wfdag import WFDAG
from wfc2dask.wftask import WFTask
import logging


def build_project(wfdag, output_directory, overwrite):
    try:
        import os
        os.makedirs(output_directory)
    except FileExistsError as exc:
        if not overwrite:
            raise exc
    import shutil
    for template in ["dask_client.py", "application.py", "helpers.py"]:
        shutil.copy("code_templates/%s" % template, "%s/%s" % (output_directory, template))
    with open("code_templates/run_workflow.py") as fp:
        run_workflow_code = fp.read()
    INDENT = "    "
    noindent_python_codelines = wfdag.dask_codelines()
    codelines = "\n".join(["%s%s" % (INDENT, codeline) for codeline in noindent_python_codelines])
    run_workflow_code = run_workflow_code.replace("# Generated code goes here", codelines)
    with open("%s/%s" % (output_directory, "run_workflow.py"), "w") as fp:
        fp.write(run_workflow_code)


def process_arguments():
    import argparse
    import sys
    parser = argparse.ArgumentParser(prog=sys.argv[0],
                                     description='Converts a workflow to dask')
    parser.add_argument("workflow_filename", help="Name of the file describing the workflow")
    parser.add_argument("-d", "--debug", help="Debug mode (Info mode by default)", action="store_true")
    parser.add_argument("-o", "--output_directory", help="Output directory name", default="out")
    parser.add_argument("-f", "--force_overwrite", help="Force overwrite if output_directory already exists", action="store_true")
    return parser.parse_args()


def main():
    args = process_arguments()
    loglevel = logging.DEBUG if args.debug else logging.INFO
    logging.basicConfig(level=loglevel, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
    # in_fn = "samples/others/wrench/simple_workflow.json"
    # in_fn = "samples/others/wrench/nighres_workflow.json"
    # in_fn = "samples/makeflow-instances/bwa-chameleon-large-003.json"
    # in_fn = "samples/makeflow-instances/bwa-chameleon-small-001.json"
    # in_fn = "samples/unittests/hello-world-join.json"
    # in_fn = "samples/unittests/hello-world-sequence.json"
    wfdag = WFDAG.from_tasks(*WFTask.load(args.workflow_filename))
    build_project(wfdag, args.output_directory, args.force_overwrite)


if __name__ == '__main__':
    main()
