import yaml , os , copy
os.chdir(os.path.dirname(os.path.abspath(__file__)))

with open("reference.yml", "r") as file:
    data = yaml.safe_load(file)

formatted_packages = []

with open("requirements.txt", "r") as f:
    for line in f:
        pkg = line.strip()
        if pkg:  # skip empty lines
            formatted_packages.append({'pypi': {'package': pkg}})

list_files = [ x for x in os.listdir() if x!= 'reference.yml' and x[-3:]=='yml']

def create_resource_file(file_name):
    with open(file_name, "r") as f:
        config = yaml.safe_load(f)

    job_name = file_name[:-4]
    job_cluster_key = job_name.replace("_", "-")

    new_data = copy.deepcopy(data)
    job_config = new_data['resources']['jobs']['automated_jobs']

    job_config['name'] = job_config['name'].replace("job_name", job_name) 
    job_config['schedule']['quartz_cron_expression'] = config['quartz_cron_expression']
    if not config['run']:
        job_config['schedule']['pause_status'] = 'PAUSED'
    job_config['email_notifications']['on_failure'].append(config['owner_email'])
    job_config['job_clusters'][0]['job_cluster_key'] = job_cluster_key
    job_config['tasks'][0]['job_cluster_key'] = job_cluster_key
    job_config['tags'].update(config['tags'])
    if config.get('language','pyspark').lower().strip()=='python':
        job_config['job_clusters'][0]['new_cluster']['spark_conf']['spark.master'] = 'local[*]'


    # Build libraries
    library = config.get('additional_libraries', [])
    library_list = [{'pypi': {'package': x}} for x in library if x]
    if library_list:
        seen = set()
        packages_to_use = []

        for pkg_list in [formatted_packages, library_list]:
            for item in pkg_list:
                pkg_name = item['pypi']['package']
                base_name = pkg_name.split("==")[0]
                if base_name not in seen:
                    seen.add(base_name)
                    packages_to_use.append(item)
    else:
        packages_to_use = formatted_packages

    # packages_to_use = library_list if library_list else formatted_packages

    job_config['tasks'][0]['task_key'] = job_name
    job_config['tasks'][0]['notebook_task']['notebook_path'] = config['notebook_path']
    job_config['tasks'][0]['libraries'] = packages_to_use

    new_data['resources']['jobs'][job_name] = job_config
    del new_data['resources']['jobs']['automated_jobs']

    with open(f"../resources/automated_{file_name}", "w") as f:
        yaml.dump(new_data, f, default_flow_style=False,sort_keys=False)


for file in list_files:
    create_resource_file(file)
