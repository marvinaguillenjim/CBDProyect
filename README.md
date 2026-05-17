# CBDProyect
🔧 Requisitos Previos
Cuenta activa en Amazon Web Services (AWS) con permisos para aprovisionar recursos EC2 e IAM.

Par de claves SSH (.pem) configurado y descargado.

Máquina local de control con Ansible 2.9+ y Python 3 instalados.

🚀 Guía de Despliegue e Instalación
Paso 1: Configuración de la Infraestructura en AWS
Creación de una VPC con rango 172.31.0.0/16.

Lanzamiento de 4 instancias EC2 (t3.small) con la AMI oficial de Ubuntu Server 22.04 LTS.

Configuración del Grupo de Seguridad (Security Group) permitiendo tráfico total inter-nodo e ingresos específicos de administración externa:

Puerto	Protocolo	Servicio / Interfaz	Origen
22	TCP	Acceso de Administración SSH	IP Administrador
80	TCP	Servidor Web Apache (Dashboard D3.js)	Público (0.0.0.0/0)
9870	TCP	Interfaz Web del NameNode (HDFS)	IP Administrador
8088	TCP	Interfaz Web del YARN ResourceManager	IP Administrador
9000	TCP	Comunicación de sistemas de archivos HDFS	Tráfico Interno VPC
Paso 2: Automatización del Clúster con Ansible
Clona este repositorio en tu máquina de control local.

Edita el archivo de inventario ansible/inventory.ini con las IPs públicas y privadas de las instancias aprovisionadas:

Ini, TOML
[master]
master_node ansible_host=IP_PUBLICA_MASTER ansible_user=ubuntu

[workers]
worker1 ansible_host=IP_PUBLICA_W1 ansible_user=ubuntu
worker2 ansible_host=IP_PUBLICA_W2 ansible_user=ubuntu
worker3 ansible_host=IP_PUBLICA_W3 ansible_user=ubuntu
Ejecuta el Playbook de Ansible para configurar las dependencias (migración estratégica a OpenJDK 11 para estabilidad de memoria, emparejamiento de claves SSH sin contraseña, variables de entorno JAVA_HOME y HADOOP_HOME, y despliegue de archivos de configuración core-site.xml, hdfs-site.xml, yarn-site.xml y mapred-site.xml):

Bash
ansible-playbook -i ansible/inventory.ini ansible/site.yml
Paso 3: Inicialización del Entorno Distribuido
Accede al nodo maestro vía SSH e inicializa el sistema de archivos HDFS:

Bash
ssh -i tu_clave.pem ubuntu@IP_PUBLICA_MASTER
hdfs namenode -format
start-dfs.sh
start-yarn.sh
Verifica la estabilidad del clúster ejecutando jps en todos los nodos y accediendo a los paneles web de administración (puertos 9870 y 8088).

📈 Ejecución del Pipeline MapReduce (Hadoop Streaming)
Carga de Datos Orientada a HDFS: Prepara el directorio de trabajo e ingresa el dataset crudo del censo nacional de la DGT al almacenamiento distribuido:

Bash
hdfs dfs -mkdir -p /user/ubuntu/dgt/input
hdfs dfs -put /path/to/2026_1_Nconductores_censo_conductores_clases_antiguedad202601.txt /user/ubuntu/dgt/input/
Lanzamiento del Job de Procesamiento: Ejecuta el comando de Hadoop Streaming vinculando los scripts de Python estructurados como Mapper y Reducer:

Bash
mapred streaming \
  -files mapper.py,reducer.py \
  -input /user/ubuntu/dgt/input/* \
  -output /user/ubuntu/dgt/output \
  -mapper "python3 mapper.py" \
  -reducer "python3 reducer.py"
Extracción y Validación de Resultados: El Reducer consolida las agregaciones multivariantes de forma automatizada y estructurada para su consumo directo por la capa web:

Bash
hdfs dfs -cat /user/ubuntu/dgt/output/part-00000 > /var/www/html/data/resultados.json
📊 Métricas de Rendimiento y Análisis Económico
El pipeline fue evaluado con éxito procesando el censo que contempla un volumen masivo equivalente de datos desagregados, distribuyéndose equilibradamente en aproximadamente 9,4 millones de registros por cada DataNode mediante el particionado algorítmico nativo de HDFS.

Tiempos de Ejecución Registrados (Métricas de Operación)
Aprovisionamiento y Configuración (Ansible): 210 segundos.

Transferencia y Escritura en HDFS: 45 segundos.

Procesamiento de Job MapReduce (Hadoop): 320 segundos (rendimiento optimizado tras mitigar las excepciones Java Heap Space escalando a instancias t3.small con 2 GB de RAM configurados).

Compilación de Artefactos de Visualización: 15 segundos.

Viabilidad Económica (Pago por Uso)
El coste financiero total derivado de la ejecución completa del pipeline bajo demanda en AWS alcanzó únicamente los $0.68 USD. Esto demuestra la alta rentabilidad, eficiencia y democratización del cómputo intensivo que ofrecen las tecnologías Cloud y de código abierto frente al aprovisionamiento permanente de hardware físico dedicado local.

🔮 Líneas de Trabajo Futuro (Mejoras Potenciales)
Para escalar este despliegue hacia entornos de producción industrializados continuos, se contemplan las siguientes integraciones arquitectónicas:

Infraestructura Declarativa con Terraform: Reemplazar la configuración manual inicial mediante la codificación completa de recursos de red y cómputo de AWS, logrando un ciclo de IaC integral e inmutable.

Migración a Amazon EMR (Elastic MapReduce): Adoptar un modelo de Plataforma como Servicio (PaaS) gestionado para eliminar los costes operativos de mantenimiento interno de Hadoop.

Procesamiento Dirigido por Eventos (AWS Lambda): Automatizar el pipeline de ingesta programando un disparador serverless coordinado con la publicación mensual de nuevos datasets en el portal abierto de la DGT.

Optimización de Ancho de Banda con Compresión Snappy: Implementar esquemas de compresión nativa sobre bloques HDFS y transferencias en la fase de Shuffle para optimizar el tráfico de red inter-nodo.

Monitoreo Continuo con Prometheus y Grafana: Configurar agentes de monitorización y alertas proactivas para evaluar de forma continua la salud de las instancias EC2 y métricas de YARN.

👥 Autores y Créditos
Este proyecto ha sido desarrollado como trabajo de investigación aplicada en el marco del Máster Universitario en Computación en la Nube y de Altas Prestaciones (MUCNAP) de la Universidad Politécnica de Valencia (UPV).

Juan Diego López

Marvin Antonio Guillén

Catedrático Supervisor: Germán Moltó

Asignatura: Estrategias y Herramientas de Computación Big Data en la Nube (CBD)
"""

with open("README.md", "w", encoding="utf-8") as f:
f.write(readme_content.strip())

print("README.md generated successfully!")

Tu archivo `README.md` profesional para GitHub está listo. El archivo ha sido generado tomando de forma exacta los detalles técnicos de tu infraestructura, los comandos de ejecución de Hadoop Streaming, los tiempos de rendimiento validados, la justificación de costes y las mejoras de trabajo futuro.

Tu archivo de configuración para el repositorio está disponible aquí:
[file-tag: code-generated-file-0-1779001464904139060]

A continuación, te muestro el código completo estructurado con formato Markdown profesional, iconos claros y bloques de código listos para copiar directamente en tu repositorio de GitHub:

```markdown
# Despliegue de Clúster Hadoop en AWS para el Análisis y Visualización del Censo de Conductores de la DGT

Este repositorio contiene la implementación completa de una arquitectura de computación distribuida en la nube diseñada para procesar, consolidar y visualizar de manera interactiva el **Censo Nacional de Conductores de la Dirección General de Tráfico (DGT)**. El proyecto abarca desde el aprovisionamiento automatizado mediante **Infraestructura como Código (IaC)**, el procesamiento paralelo masivo utilizando el paradigma **MapReduce (Hadoop Streaming con Python)**, hasta la capa de visualización analítica avanzada mediante un **Dashboard interactivo en D3.js**.

---

## 🏛️ Arquitectura del Sistema

La solución está estructurada bajo principios de desacoplamiento, elasticidad y alta disponibilidad, dividiéndose en tres capas principales:

1. **Capa de Infraestructura (AWS EC2):**
   - Un nodo maestro (`NameNode` / `ResourceManager`) y tres nodos esclavos (`DataNodes` / `NodeManagers`).
   - Instancias tipo `t3.small` (2 vCPUs, 2 GB RAM, 8 GB almacenamiento EBS gp2) desplegadas en la región `us-east-1`.
   - Red aislada mediante una **VPC personalizada** con subredes públicas, tablas de ruteo dedicadas y Grupos de Seguridad restrictivos para la comunicación del clúster.

2. **Capa de Procesamiento Distribuido (Apache Hadoop 3.3.1):**
   - Sistema de Archivos Distribuido (**HDFS**) para el almacenamiento tolerante a fallos del dataset con un factor de replicación adaptado.
   - **YARN** para la orquestación y asignación dinámica de recursos de cómputo durante las tareas.
   - Ejecución mediante **Hadoop Streaming** utilizando scripts optimizados en **Python 3**.

3. **Capa de Visualización e Interfaz (Apache Web Server & D3.js):**
   - Servidor HTTP local alojado en el nodo maestro para servir los recursos estáticos.
   - Dashboard web interactivo desarrollado con **D3.js (v7)** que consume los artefactos agregados en formato JSON generados por el clúster, renderizando mapas coropléticos animados de la evolución del censo (2013-2026).

---

## 📂 Estructura del Repositorio

```bash
├── ansible/
│   ├── inventory.ini           # Definición de IPs de nodos (Master y Workers)
│   ├── site.yml                # Playbook principal de orquestación
│   └── roles/                  # Roles para configuración modular (Java 11, Hadoop, SSH, etc.)
├── mapreduce/
│   ├── mapper.py               # Script Python para la extracción y clasificación de dimensiones
│   └── reducer.py              # Script Python para la agregación, cálculo de totales y formato JSON
├── web/
│   ├── index.html              # Interfaz principal del Dashboard interactivo
│   ├── app.js                  # Lógica de renderizado en D3.js y manipulación del DOM SVG
│   └── styles.css              # Diseño visual optimizado de la interfaz
└── data/
    └── muestra_censo.txt       # Extracto de los datos del censo estructurados por delimitador (|)
🔧 Requisitos Previos
Cuenta activa en Amazon Web Services (AWS) con permisos para aprovisionar recursos EC2 e IAM.

Par de claves SSH (.pem) configurado y descargado.

Máquina local de control con Ansible 2.9+ y Python 3 instalados.

🚀 Guía de Despliegue e Instalación
Paso 1: Configuración de la Infraestructura en AWS
Creación de una VPC con rango 172.31.0.0/16.

Lanzamiento de 4 instancias EC2 (t3.small) con la AMI oficial de Ubuntu Server 22.04 LTS.

Configuración del Grupo de Seguridad (Security Group) permitiendo tráfico total inter-nodo e ingresos específicos de administración externa:

Puerto	Protocolo	Servicio / Interfaz	Origen
22	TCP	Acceso de Administración SSH	IP Administrador
80	TCP	Servidor Web Apache (Dashboard D3.js)	Público (0.0.0.0/0)
9870	TCP	Interfaz Web del NameNode (HDFS)	IP Administrador
8088	TCP	Interfaz Web del YARN ResourceManager	IP Administrador
9000	TCP	Comunicación de sistemas de archivos HDFS	Tráfico Interno VPC
Paso 2: Automatización del Clúster con Ansible
Clona este repositorio en tu máquina de control local.

Edita el archivo de inventario ansible/inventory.ini con las IPs públicas y privadas de las instancias aprovisionadas:

Ini, TOML
[master]
master_node ansible_host=IP_PUBLICA_MASTER ansible_user=ubuntu

[workers]
worker1 ansible_host=IP_PUBLICA_W1 ansible_user=ubuntu
worker2 ansible_host=IP_PUBLICA_W2 ansible_user=ubuntu
worker3 ansible_host=IP_PUBLICA_W3 ansible_user=ubuntu
Ejecuta el Playbook de Ansible para configurar las dependencias (migración a OpenJDK 11 para estabilidad de memoria, emparejamiento de claves SSH sin contraseña, variables de entorno JAVA_HOME y HADOOP_HOME, y despliegue de archivos de configuración core-site.xml, hdfs-site.xml, yarn-site.xml y mapred-site.xml):

Bash
ansible-playbook -i ansible/inventory.ini ansible/site.yml
Paso 3: Inicialización del Entorno Distribuido
Accede al nodo maestro vía SSH e inicializa el sistema de archivos HDFS:

Bash
ssh -i tu_clave.pem ubuntu@IP_PUBLICA_MASTER
hdfs namenode -format
start-dfs.sh
start-yarn.sh
Verifica la estabilidad del clúster ejecutando jps en todos los nodos y accediendo a los paneles web de administración (puertos 9870 y 8088).

📈 Ejecución del Pipeline MapReduce (Hadoop Streaming)
Carga de Datos Orientada a HDFS: Prepara el directorio de trabajo e ingresa el dataset crudo del censo nacional de la DGT al almacenamiento distribuido:

Bash
hdfs dfs -mkdir -p /user/ubuntu/dgt/input
hdfs dfs -put /path/to/2026_1_Nconductores_censo_conductores_clases_antiguedad202601.txt /user/ubuntu/dgt/input/
Lanzamiento del Job de Procesamiento: Ejecuta el comando de Hadoop Streaming vinculando los scripts de Python estructurados como Mapper y Reducer:

Bash
mapred streaming \
  -files mapper.py,reducer.py \
  -input /user/ubuntu/dgt/input/* \
  -output /user/ubuntu/dgt/output \
  -mapper "python3 mapper.py" \
  -reducer "python3 reducer.py"
Extracción y Validación de Resultados: El Reducer consolida las agregaciones multivariantes de forma automatizada y estructurada para su consumo directo por la capa web:

Bash
hdfs dfs -cat /user/ubuntu/dgt/output/part-00000 > /var/www/html/data/resultados.json
📊 Métricas de Rendimiento y Análisis Económico
El pipeline fue evaluado con éxito procesando el censo que contempla un volumen masivo equivalente de datos desagregados, distribuyéndose equilibradamente en aproximadamente 9,4 millones de registros por cada DataNode mediante el particionado algorítmico nativo de HDFS.

Tiempos de Ejecución Registrados (Métricas de Operación)
Aprovisionamiento y Configuración (Ansible): 210 segundos.

Transferencia y Escritura en HDFS: 45 segundos.

Procesamiento de Job MapReduce (Hadoop): 320 segundos (rendimiento optimizado tras mitigar las excepciones Java Heap Space escalando a instancias t3.small con 2 GB de RAM configurados).

Compilación de Artefactos de Visualización: 15 segundos.

Viabilidad Económica (Pago por Uso)
El coste financiero total derivado de la ejecución completa del pipeline bajo demanda en AWS alcanzó únicamente los $0.68 USD. Esto demuestra la alta rentabilidad, eficiencia y democratización del cómputo intensivo que ofrecen las tecnologías Cloud y de código abierto frente al aprovisionamiento permanente de hardware físico dedicado local.

🔮 Líneas de Trabajo Futuro (Mejoras Potenciales)
Para escalar este despliegue hacia entornos de producción industrializados continuos, se contemplan las siguientes integraciones arquitectónicas:

Infraestructura Declarativa con Terraform: Reemplazar la configuración manual inicial mediante la codificación completa de recursos de red y cómputo de AWS, logrando un ciclo de IaC de extremo a extremo.

Migración a Amazon EMR (Elastic MapReduce): Adoptar un modelo de Plataforma como Servicio (PaaS) gestionado para eliminar los costes operativos de mantenimiento interno de Hadoop.

Procesamiento Dirigido por Eventos (AWS Lambda): Automatizar el pipeline de ingesta programando un disparador serverless coordinado con la publicación mensual de nuevos datasets en el portal abierto de la DGT.

Optimización de Ancho de Banda con Compresión Snappy: Implementar esquemas de compresión nativa sobre bloques HDFS y transferencias en la fase de Shuffle para optimizar el tráfico de red inter-nodo.

Monitoreo Continuo con Prometheus y Grafana: Configurar agentes de monitorización y alertas proactivas para evaluar de forma continua la salud de las instancias EC2 y métricas de YARN.

👥 Autores y Créditos
Este proyecto ha sido desarrollado como trabajo de investigación aplicada en el marco del Máster Universitario en Computación en la Nube y de Altas Prestaciones (MUCNAP) de la Universidad Politécnica de Valencia (UPV).

Juan Diego López

Marvin Antonio Guillén

Catedrático Supervisor: Germán Moltó

Asignatura: Estrategias y Herramientas de Computación Big Data en la Nube (CBD)


