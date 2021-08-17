import io
import json
import os
import requests
import subprocess
import xml.etree.ElementTree as ET
import zipfile

def run_shell_command(cmd):
    process = subprocess.run(cmd.split(" "))
    assert process.returncode == 0

# Start CDAP sandbox
print("Downloading CDAP sandbox")
sandbox_url = "https://builds.cask.co/artifact/CDAP-BUT/shared/build-latestSuccessful/SDK/cdap/cdap-standalone/target/cdap-sandbox-6.6.0-SNAPSHOT.zip"
sandbox_dir = sandbox_url.split("/")[-1].split(".zip")[0]
r = requests.get(sandbox_url)
z = zipfile.ZipFile(io.BytesIO(r.content))
z.extractall("./sandbox")
print("Start the sandbox")
run_shell_command(f"chmod +x sandbox/{sandbox_dir}/bin/cdap")
run_shell_command(f"sandbox/{sandbox_dir}/bin/cdap sandbox start")

# Build the plugin
os.chdir("plugin")
print("Building plugin")
run_shell_command("mvn clean package -DskipTests")

# Get plugin artifact name and version from pom.xml.
root = ET.parse('pom.xml').getroot()
plugin_name = root.find('{http://maven.apache.org/POM/4.0.0}artifactId').text
plugin_version = root.find('{http://maven.apache.org/POM/4.0.0}version').text

os.chdir("target")
plugin_properties = {}
plugin_parents = []
# Get plugin properties and parent from plugin json.
with open(f'{plugin_name}-{plugin_version}.json') as f:
    obj = json.loads(f.read())
    plugin_properties = obj['properties']
    plugin_parents = obj['parents']

data = None
with open(f'{plugin_name}-{plugin_version}.jar', 'rb') as f:
    data = f.read()

# Install the plugin on the sandbox.
print("Installing plugin")
res=requests.post(f"http://localhost:11015/v3/namespaces/default/artifacts/{plugin_name}", headers={"Content-TYpe": "application/octet-stream", "Artifact-Extends": '/'.join(plugin_parents), "Artifact-Version": plugin_version}, data=data)
assert res.ok or print(res.text)
res=requests.put(f"http://localhost:11015/v3/namespaces/default/artifacts/{plugin_name}/versions/{plugin_version}/properties", json=plugin_properties)
assert res.ok or print(res.texts)

os.chdir("../..")
print("cwd:", os.getcwd())
print("ls:", os.listdir())

# Run e2e tests
print("Running e2e tests")
os.chdir("e2e")
run_shell_command("mvn clean test")
