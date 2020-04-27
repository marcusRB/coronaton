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

