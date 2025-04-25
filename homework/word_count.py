"""Taller presencial"""

"""Taller presencial"""

# pylint: disable=broad-exception-raised

import fileinput
import glob
import itertools
import os
import os.path
import string
import time
from collections import defaultdict
from multiprocessing import Pool # Permite la ejecución en paralelo

from toolz.itertoolz import concat  # type: ignore


#
# Copia de archivos
#

# Función igual a la de el word_count del taller 2
def copy_raw_files_to_input_folder(n):
    """Funcion copy_files"""

    create_directory(directory="files/input")

    for file in glob.glob("files/raw/*"):
        for i in range(1, n + 1):
            with open(file, "r", encoding="utf-8") as f:
                with open(
                    f"files/input/{os.path.basename(file).split('.')[0]}_{i}.txt",
                    "w",
                    encoding="utf-8",
                ) as f2:
                    f2.write(f.read())


#
# Lectura de archivos
#
def load_input(input_directory):
    """Funcion load_input"""

    def make_iterator_from_single_file(input_directory):
        # Si el directorio es un archivo sencillo, lo lee y usa
        # yield 
        with open(input_directory, "rt", encoding="utf-8") as file:
            yield from file

    def make_iterator_from_multiple_files(input_directory):
        # Si el directorio es un directorio, lee todos los archivos,
        # los obtiene con glob y usa yield from para leerlos una línea
        # a la vez.
        input_directory = os.path.join(input_directory, "*")
        files = glob.glob(input_directory)
        with fileinput.input(files=files) as file:
            yield from file

    # Verifica si el directorio es un archivo
    if os.path.isfile(input_directory):
        return make_iterator_from_single_file(input_directory)

    # Si no es un archivo, asume que es un directorio
    return make_iterator_from_multiple_files(input_directory)


#
# Preprocesamiento
#
def preprocessing(x):
    """Preprocess the line x"""
    x = x.lower()
    x = x.translate(str.maketrans("", "", string.punctuation))
    x = x.replace("\n", "")
    return x


def line_preprocessing(sequence):
    """Preprocess the lines"""
    # Pool detecta la cantidad de CPUs disponibles y las usa para hacer el
    # procesamiento en paralelo.
    with Pool() as pool:
        # Divide sequence en partes iguales para cada CPU
        # y las procesa en paralelo.
        return pool.map(preprocessing, sequence)


#
# Mapper
#
def map_line(x):
    # Divide la línea en palabras y devuelve una lista de tuplas (palabra, 1)
    # para cada palabra en la línea.
    return [(w, 1) for w in x.split()]


def mapper(sequence):
    """Mapper"""

    with Pool() as pool:
        sequence = pool.map(map_line, sequence)
        # Una las listas de tuplas para que solo haya una lista de tuplas
        # en vez de una lista de listas de tuplas.
        sequence = concat(sequence)

    return sequence


#
# Shuffle and Sort
#
# Shuffle and Sort igual al del taller 2.
def shuffle_and_sort(sequence):
    """Shuffle and Sort"""
    return sorted(sequence, key=lambda x: x[0])


#
# Reducer
#
def sum_by_key(chunk):
    """Sum (key, value) tuples by key"""
    # Suma los valores de las tuplas (key, value) por key.
    # Usa defaultdict para inicializar el valor en 0 si no existe.
    result = defaultdict(int)
    for key, value in chunk:
        result[key] += value
    # Devuelve una lista de tuplas (key, value) con el resultado.
    return list(result.items())


def reducer(sequence):
    """Reducer"""

    def chunkify(sequence, num_chunks):
        return [sequence[i::num_chunks] for i in range(num_chunks)]

    def merge_results(chunks):
        # Una vez que cada chunk ha sido procesado, combina los resultados
        # en un solo diccionario.
        final_result = defaultdict(int)
        for chunk in chunks:
            for key, value in chunk:
                final_result[key] += value
        return list(final_result.items())

    # Define el número de chunks como el número de CPUs disponibles
    num_chunks = os.cpu_count()
    # Divide la secuencia en partes iguales para cada CPU
    chunks = chunkify(sequence, num_chunks)

    # Usa Pool para procesar cada chunk en paralelo
    with Pool(num_chunks) as pool:
        # Procesa cada chunk en paralelo y devuelve una lista de resultados
        chunk_results = pool.map(sum_by_key, chunks)

    return merge_results(chunk_results)


#
# Crea el directory de salida
#
def create_directory(directory):
    """Create Output Directory"""

    if os.path.exists(directory):
        for file in glob.glob(f"{directory}/*"):
            os.remove(file)
        os.rmdir(directory)
    os.makedirs(directory)


#
# Guarda el resultado en un archivo
#
def save_output(output_directory, sequence):
    """Save Output"""
    with open(f"{output_directory}/part-00000", "w", encoding="utf-8") as f:
        for key, value in sequence:
            f.write(f"{key}\t{value}\n")


#
# La siguiente función crea un archivo llamado _SUCCESS en el directorio
# entregado como parámetro.
#
def create_marker(output_directory):
    """Create Marker"""
    with open(f"{output_directory}/_SUCCESS", "w", encoding="utf-8") as f:
        f.write("")


#
# Escriba la función job, la cual orquesta las funciones anteriores.
#
def run_job(input_directory, output_directory):
    """Job"""
    sequence = load_input(input_directory)
    sequence = line_preprocessing(sequence)
    sequence = mapper(sequence)
    sequence = shuffle_and_sort(sequence)
    sequence = reducer(sequence)

    create_directory(output_directory)
    save_output(output_directory, sequence)
    create_marker(output_directory)


if __name__ == "__main__":

    copy_raw_files_to_input_folder(n=1000)

    start_time = time.time()

    run_job(
        "files/input",
        "files/output",
    )

    end_time = time.time()
    print(f"Tiempo de ejecución: {end_time - start_time:.2f} segundos")