#include <stdio.h>     // printf(), perror(), fgets()
#include <stdlib.h>    // exit(), atoi()
#include <string.h>    // strtok(), strcmp(), strlen(), strchr()
#include <unistd.h>    // fork(), execvp(), pipe(), dup2(), close()
#include <sys/types.h> // pid_t
#include <sys/wait.h>  // waitpid(), WNOHANG
#include <errno.h>     // EINTR, ECHILD
#include <signal.h>    // kill(), SIGTERM
#include <readline/readline.h>
#include <readline/history.h>

// ============================================================
// CONSTANTES
// ============================================================

#define MAX_LINE 1024
#define MAX_ARGS 64
#define MAX_COMMANDS 16
#define MAX_JOBS 64

// ============================================================
// ESTRUCTURA DE LOS TRABAJOS
// ============================================================

// Representa un trabajo ejecutado en segundo plano.
// Puede contener varios procesos si se trata de un pipeline.

typedef struct
{
    int id;                   // Identificador del trabajo: fg %1
    pid_t pids[MAX_COMMANDS]; // PID de todos sus procesos
    int num_pids;             // Cantidad de procesos
    char comando[MAX_LINE];   // Comando original
    int activo;               // 1 = activo, 0 = terminado
} Trabajo;

// ============================================================
// VARIABLES GLOBALES
// ============================================================

// Registro de los trabajos ejecutados en segundo plano.
Trabajo lista_trabajos[MAX_JOBS];

// Identificador que se asignará al siguiente trabajo.
int siguiente_id = 1;

// ============================================================
// FUNCIÓN: buscar_espacio_trabajo()
// ============================================================

// Busca una posición disponible en el registro.
// Permite reutilizar posiciones de trabajos terminados.

int buscar_espacio_trabajo()
{

    for (int i = 0; i < MAX_JOBS; i++)
    {

        if (!lista_trabajos[i].activo)
        {
            return i;
        }
    }

    return -1;
}

// ============================================================
// FUNCIÓN: actualizar_trabajos()
// ============================================================

// Comprueba si los procesos de los trabajos en segundo
// plano ya terminaron, sin bloquear el Shell.

void actualizar_trabajos()
{

    // Recorremos todos los trabajos registrados.
    for (int i = 0; i < MAX_JOBS; i++)
    {

        if (lista_trabajos[i].activo)
        {

            // Revisamos todos los procesos del trabajo.
            for (int j = 0; j < lista_trabajos[i].num_pids; j++)
            {

                pid_t pid = lista_trabajos[i].pids[j];

                // Si el PID es 0, el proceso ya fue recogido.
                if (pid == 0)
                {
                    continue;
                }

                int resultado;

                // Consultamos el proceso sin bloquear el Shell.
                // Si la espera se interrumpe, intentamos nuevamente.
                do
                {
                    resultado = waitpid(pid, NULL, WNOHANG);
                } while (resultado == -1 && errno == EINTR);

                // El proceso terminó.
                if (resultado == pid)
                {
                    lista_trabajos[i].pids[j] = 0;
                }

                // Ya no existe un proceso hijo que podamos esperar.
                else if (resultado == -1 && errno == ECHILD)
                {
                    lista_trabajos[i].pids[j] = 0;
                }

                // Si devuelve 0, el proceso sigue ejecutándose.
            }

            // Comprobamos si todavía queda algún proceso activo.
            int quedan_procesos = 0;

            for (int j = 0; j < lista_trabajos[i].num_pids; j++)
            {

                if (lista_trabajos[i].pids[j] != 0)
                {
                    quedan_procesos = 1;
                    break;
                }
            }

            // Si todos terminaron, el trabajo deja de estar activo.
            if (!quedan_procesos)
            {
                lista_trabajos[i].activo = 0;
            }
        }
    }
}

// ============================================================
// FUNCIÓN: mostrar_jobs()
// ============================================================

// Implementa el comando interno jobs.
// Muestra los trabajos que siguen ejecutándose.

void mostrar_jobs()
{

    // Actualizamos los estados antes de mostrar los trabajos.
    actualizar_trabajos();

    printf("--- Trabajos en segundo plano ---\n");

    int encontrados = 0;

    for (int i = 0; i < MAX_JOBS; i++)
    {

        if (lista_trabajos[i].activo)
        {

            encontrados = 1;

            printf("[%d] PID:", lista_trabajos[i].id);

            // Mostramos los PID que todavía están pendientes.
            for (int j = 0; j < lista_trabajos[i].num_pids; j++)
            {

                if (lista_trabajos[i].pids[j] != 0)
                {
                    printf(" %d", lista_trabajos[i].pids[j]);
                }
            }

            printf(" - %s\n", lista_trabajos[i].comando);
        }
    }

    if (!encontrados)
    {
        printf("No hay trabajos activos.\n");
    }
}

// ============================================================
// FUNCIÓN: traer_a_fg()
// ============================================================

// Implementa fg %n.
// Busca el trabajo indicado y espera por sus procesos pendientes.

void traer_a_fg(int id)
{

    // Actualizamos los trabajos antes de buscar.
    actualizar_trabajos();

    // Buscamos el trabajo por su identificador.
    for (int i = 0; i < MAX_JOBS; i++)
    {

        if (lista_trabajos[i].id == id &&
            lista_trabajos[i].activo)
        {

            printf("Trayendo trabajo [%d] al primer plano...\n", id);

            // Esperamos por cada proceso del pipeline.
            for (int j = 0; j < lista_trabajos[i].num_pids; j++)
            {

                pid_t pid = lista_trabajos[i].pids[j];

                // Si ya terminó, pasamos al siguiente.
                if (pid == 0)
                {
                    continue;
                }

                int resultado;

                // Esperamos hasta que termine el proceso.
                // Sin WNOHANG, el Shell queda bloqueado esperando.
                do
                {
                    resultado = waitpid(pid, NULL, 0);
                } while (resultado == -1 && errno == EINTR);

                if (resultado == pid)
                {
                    lista_trabajos[i].pids[j] = 0;
                }

                else if (resultado == -1 && errno == ECHILD)
                {
                    lista_trabajos[i].pids[j] = 0;
                }

                else if (resultado == -1)
                {
                    perror("Error esperando proceso");
                }
            }

            // Actualizamos el estado del trabajo.
            actualizar_trabajos();

            return;
        }
    }

    printf("Error: No se encontro el trabajo con ID %d\n", id);
}

// ============================================================
// FUNCIÓN: ejecutar_pipeline()
// ============================================================

// Ejecuta uno o varios comandos conectados mediante pipes.
// Administra su ejecución en primer o segundo plano.

void ejecutar_pipeline(
    char *comandos[],
    int num_comandos,
    int en_segundo_plano,
    char *linea_original)
{

    // Validamos la cantidad de comandos.
    if (num_comandos < 1 || num_comandos > MAX_COMMANDS)
    {
        printf("Error: cantidad de comandos invalida.\n");
        return;
    }

    // Para N comandos necesitamos N-1 pipes.
    int num_pipes = num_comandos - 1;

    // Arreglo de pipes.
    // Su tamaño fijo también permite ejecutar un solo comando.
    int fd[MAX_COMMANDS - 1][2];

    // --------------------------------------------------------
    // CREACIÓN DE LOS PIPES
    // --------------------------------------------------------

    for (int i = 0; i < num_pipes; i++)
    {

        if (pipe(fd[i]) < 0)
        {

            perror("Error creando pipe");

            // Cerramos los pipes que se crearon anteriormente.
            for (int j = 0; j < i; j++)
            {
                close(fd[j][0]);
                close(fd[j][1]);
            }

            return;
        }
    }

    // Aquí guardaremos los PID de los procesos creados.
    pid_t pids[MAX_COMMANDS];

    int procesos_creados = 0;

    // --------------------------------------------------------
    // CREACIÓN DE LOS PROCESOS
    // --------------------------------------------------------

    for (int i = 0; i < num_comandos; i++)
    {

        pids[i] = fork();

        // Si fork falla, detenemos la creación.
        if (pids[i] < 0)
        {

            perror("Error en fork");

            // Terminamos los procesos que ya se crearon.
            for (int j = 0; j < procesos_creados; j++)
            {
                kill(pids[j], SIGTERM);
            }

            // Cerramos todos los pipes.
            for (int j = 0; j < num_pipes; j++)
            {
                close(fd[j][0]);
                close(fd[j][1]);
            }

            // Recogemos los procesos creados.
            for (int j = 0; j < procesos_creados; j++)
            {
                waitpid(pids[j], NULL, 0);
            }

            return;
        }

        // ----------------------------------------------------
        // PROCESO HIJO
        // ----------------------------------------------------

        if (pids[i] == 0)
        {

            // Si no es el primer comando, recibe la entrada
            // desde el pipe anterior.
            if (i > 0)
            {

                if (dup2(fd[i - 1][0], STDIN_FILENO) < 0)
                {
                    perror("Error redirigiendo entrada");
                    _exit(1);
                }
            }

            // Si no es el último comando, envía su salida
            // hacia el siguiente pipe.
            if (i < num_comandos - 1)
            {

                if (dup2(fd[i][1], STDOUT_FILENO) < 0)
                {
                    perror("Error redirigiendo salida");
                    _exit(1);
                }
            }

            // Cerramos los extremos originales de los pipes.
            // Las conexiones ya fueron configuradas con dup2().
            for (int j = 0; j < num_pipes; j++)
            {
                close(fd[j][0]);
                close(fd[j][1]);
            }

            // ------------------------------------------------
            // SEPARACIÓN DE LOS ARGUMENTOS
            // ------------------------------------------------

            char *args[MAX_ARGS];

            int arg_idx = 0;

            // Separamos el comando utilizando espacios
            // y tabulaciones.
            char *token = strtok(comandos[i], " \t\n");

            while (token != NULL)
            {

                // Reservamos una posición para el NULL final.
                if (arg_idx >= MAX_ARGS - 1)
                {
                    fprintf(stderr, "Demasiados argumentos.\n");
                    _exit(1);
                }

                args[arg_idx++] = token;

                token = strtok(NULL, " \t\n");
            }

            // execvp necesita que el arreglo termine en NULL.
            args[arg_idx] = NULL;

            // Si no hay comando, terminamos el proceso hijo.
            if (args[0] == NULL)
            {
                _exit(0);
            }

            // Ejecutamos el comando solicitado.
            if (execvp(args[0], args) < 0)
            {
                perror("Error ejecutando comando");
                _exit(127);
            }
        }

        // ----------------------------------------------------
        // PROCESO PADRE
        // ----------------------------------------------------

        // Guardamos la cantidad de procesos creados.
        procesos_creados++;
    }

    // --------------------------------------------------------
    // CIERRE DE PIPES EN EL PADRE
    // --------------------------------------------------------

    // El padre no necesita utilizar los pipes.
    for (int i = 0; i < num_pipes; i++)
    {
        close(fd[i][0]);
        close(fd[i][1]);
    }

    // --------------------------------------------------------
    // EJECUCIÓN EN SEGUNDO PLANO
    // --------------------------------------------------------

    if (en_segundo_plano)
    {

        // Buscamos una posición disponible para el trabajo.
        int espacio = buscar_espacio_trabajo();

        if (espacio == -1)
        {

            printf("No hay espacio para registrar mas trabajos.\n");

            // Si no podemos registrarlo, esperamos por sus procesos.
            for (int i = 0; i < num_comandos; i++)
            {
                waitpid(pids[i], NULL, 0);
            }

            return;
        }

        // Obtenemos la posición disponible.
        Trabajo *trabajo = &lista_trabajos[espacio];

        // Inicializamos los datos del trabajo.
        memset(trabajo, 0, sizeof(Trabajo));

        trabajo->id = siguiente_id++;

        trabajo->num_pids = num_comandos;

        trabajo->activo = 1;

        // Guardamos el comando original.
        strncpy(
            trabajo->comando,
            linea_original,
            MAX_LINE - 1);

        trabajo->comando[MAX_LINE - 1] = '\0';

        // Guardamos todos los PID del pipeline.
        for (int i = 0; i < num_comandos; i++)
        {
            trabajo->pids[i] = pids[i];
        }

        // Mostramos el ID del trabajo y el PID del último proceso.
        printf("[%d] %d\n", trabajo->id, pids[num_comandos - 1]);
    }

    // --------------------------------------------------------
    // EJECUCIÓN EN PRIMER PLANO
    // --------------------------------------------------------

    else
    {

        // El padre espera por todos los procesos del pipeline.
        for (int i = 0; i < num_comandos; i++)
        {

            int resultado;

            do
            {
                resultado = waitpid(pids[i], NULL, 0);
            } while (resultado == -1 && errno == EINTR);

            if (resultado == -1 && errno != ECHILD)
            {
                perror("Error esperando proceso");
            }
        }
    }
}

// ============================================================
// FUNCIÓN PRINCIPAL: main()
// ============================================================

int main()
{

    char linea[MAX_LINE];

    char linea_copia[MAX_LINE];

    while (1)
    {

        // Actualizamos los trabajos antes de mostrar el prompt.
        actualizar_trabajos();

        // Leemos el comando introducido por el usuario.
        char *entrada = readline("mi_shell> ");

        if (entrada == NULL)
        {
            break;
        }

        strncpy(linea, entrada, MAX_LINE - 1);
        linea[MAX_LINE - 1] = '\0';

        if (strlen(linea) > 0)
        {
            add_history(linea);
        }

        free(entrada);

        // Eliminamos el salto de línea.
        linea[strcspn(linea, "\n")] = '\0';

        // Ignoramos las líneas vacías.
        if (strlen(linea) == 0)
        {
            continue;
        }

        // Eliminamos espacios al inicio y al final.
        char *inicio = linea;

        while (*inicio == ' ' || *inicio == '\t')
        {
            inicio++;
        }

        if (*inicio == '\0')
        {
            continue;
        }

        // Movemos el contenido al inicio del arreglo.
        if (inicio != linea)
        {
            memmove(linea, inicio, strlen(inicio) + 1);
        }

        // ----------------------------------------------------
        // COMANDO INTERNO: exit
        // ----------------------------------------------------

        if (strcmp(linea, "exit") == 0)
        {
            break;
        }

        // Conservamos el comando original para registrarlo.
        strncpy(linea_copia, linea, MAX_LINE - 1);
        linea_copia[MAX_LINE - 1] = '\0';

        // ----------------------------------------------------
        // DETECCIÓN DEL SEGUNDO PLANO: &
        // ----------------------------------------------------

        int en_segundo_plano = 0;

        int len = strlen(linea);

        // Detectamos si el comando termina en &.
        if (linea[len - 1] == '&')
        {

            en_segundo_plano = 1;

            // Eliminamos el símbolo para procesar los comandos.
            linea[len - 1] = '\0';

            len--;

            // Eliminamos espacios antes del símbolo &.
            while (len > 0 &&
                   (linea[len - 1] == ' ' || linea[len - 1] == '\t'))
            {

                linea[len - 1] = '\0';
                len--;
            }
        }

        // Validamos que exista un comando.
        if (strlen(linea) == 0)
        {
            printf("Error: comando vacio.\n");
            continue;
        }

        // ----------------------------------------------------
        // COMANDO INTERNO: jobs
        // ----------------------------------------------------

        if (strcmp(linea, "jobs") == 0)
        {

            mostrar_jobs();

            continue;
        }

        // ----------------------------------------------------
        // COMANDO INTERNO: fg %n
        // ----------------------------------------------------

        if (strncmp(linea, "fg %", 4) == 0)
        {

            int job_id = atoi(&linea[4]);

            if (job_id > 0)
            {
                traer_a_fg(job_id);
            }

            else
            {
                printf("Error: formato invalido. Uso: fg %%n\n");
            }

            continue;
        }

        // ----------------------------------------------------
        // SEPARACIÓN DE LOS COMANDOS DEL PIPELINE
        // ----------------------------------------------------

        char *comandos[MAX_COMMANDS];

        int num_comandos = 0;

        char *inicio_comando = linea;

        // Separamos los comandos utilizando el símbolo |.
        // También verificamos que no existan comandos vacíos.

        while (1)
        {

            char *separador = strchr(inicio_comando, '|');

            // Si encontramos un pipe, separamos la cadena.
            if (separador != NULL)
            {
                *separador = '\0';
            }

            // Eliminamos espacios al inicio del comando.
            while (*inicio_comando == ' ' ||
                   *inicio_comando == '\t')
            {
                inicio_comando++;
            }

            // Buscamos el final del comando.
            int longitud = strlen(inicio_comando);

            while (longitud > 0 &&
                   (inicio_comando[longitud - 1] == ' ' ||
                    inicio_comando[longitud - 1] == '\t'))
            {

                inicio_comando[longitud - 1] = '\0';
                longitud--;
            }

            // No permitimos comandos vacíos.
            if (longitud == 0)
            {

                printf("Error: pipeline invalido, hay un comando vacio.\n");

                num_comandos = 0;

                break;
            }

            // Validamos el límite de comandos.
            if (num_comandos >= MAX_COMMANDS)
            {

                printf("Error: se excedio el maximo de comandos.\n");

                num_comandos = 0;

                break;
            }

            // Guardamos el comando separado.
            comandos[num_comandos++] = inicio_comando;

            // Si no hay otro pipe, terminamos.
            if (separador == NULL)
            {
                break;
            }

            // Continuamos después del pipe.
            inicio_comando = separador + 1;
        }

        // Si la separación fue correcta, ejecutamos el pipeline.
        if (num_comandos > 0)
        {

            ejecutar_pipeline(
                comandos,
                num_comandos,
                en_segundo_plano,
                linea_copia);
        }
    }

    return 0;
}
