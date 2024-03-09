namespace Business
{
    public interface IGeneralProcess
    {
        /// <summary>
        /// Proceso principal del apliocativo en el cual se llaman las demas funciones y es llamado por el servicio windows o el proyecto de prueba (aplicacion de consola)
        /// </summary>
        /// <returns> void </returns>
        void MainProcess();
    }
}