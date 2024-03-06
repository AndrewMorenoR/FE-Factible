using Model.XmlModel;
using System.Collections.Generic;

namespace Business
{
    public interface IXmlProcess
    {
        /// <summary>
        /// Proceso principal del apliocativo en el cual se llaman las demas funciones y es llamado por el servicio windows o el proyecto de prueba (aplicacion de consola)
        /// </summary>
        /// <param name="data">Objeto del tipo Xmldocumento con la estructura completa de la data a procesar</param>
        /// <returns> Devuelve el Xml generado a forma de string con la estructura del objeto recibido y convertido en base 64 </returns>
        string GetBase64Xml(Xmldocumento data);
    }
}