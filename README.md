# CORONATON 2020
 coronaton-docker
 
Pasos a seguir para la creación del docker en Container Registry en SHELL

* * *
## Entorno CLOUD SHELL

### CREATE Imagen
```
docker build -t coronascript .
``` 

### Etiquetado

```
docker tag coronascript gcr.io/[PROJECT-ID]/coronascript
```


### PUSH de la imagen

```
docker push gcr.io/[PROJECT-ID]/coronascript
```

* * *

## Entorno VM

## credential docker en VM
```
docker-credential-gcr configure-docker
```

### PULL

```
docker pull gcr.io/[PROJECT-ID]/coronascript
```

## execute docker

```
time docker run gcr.io/aischool-272715/coronascript
```

* * *
## Entorno Cloud Storage

- La carpeta `/data` para los archivos (natalidad*, race*, sex*)

- La carpeta `/output` para la salida del fichero generado tipo *name@hostname.ext* 


## Permisos IAM - Service Account habilitado en dev

- Editor
- Cloud Storage Creator
- certificado de registry


## Virtual Machine habilitada



