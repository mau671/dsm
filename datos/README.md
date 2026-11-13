# Datos de prueba

Material real usado por las aplicaciones de prueba. Nada de esto se genera
durante la ejecución: los archivos están versionados para que las pruebas
trabajen siempre sobre los mismos bytes.

## `video_128x96.rgb`

48 cuadros de 128 × 96 píxeles en RGB24, precedidos de la cabecera de tres
enteros de 32 bits que espera `filtro_video` (ancho, alto y cantidad de
cuadros).

Procede de un video real convertido con ffmpeg:

```sh
ffmpeg -i origen.webm -t 0.8 -vf scale=128:96 -pix_fmt rgb24 -f rawvideo crudo.rgb
video_pack crudo.rgb datos/video_128x96.rgb 128 96
```

## `tablero_192.gray`

Una fotografía en escala de grises de 192 × 192 píxeles, un byte por píxel.
`juego_vida --imagen` la lee como estado inicial del autómata: cada píxel con
valor mayor o igual a 128 queda como célula viva.

```sh
ffmpeg -i origen.jpg -vf scale=192:192 -pix_fmt gray -f rawvideo tablero_192.gray
```
