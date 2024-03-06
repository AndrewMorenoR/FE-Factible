using Log;
using Model.XmlModel;
using System;
using System.Collections.Generic;
using System.Configuration;
using System.Diagnostics;
using System.IO;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using System.Xml;
using System.Xml.Serialization;

namespace Business
{
    public class XmlProcess : IXmlProcess
    {
        private string XmlPath = ConfigurationSettings.AppSettings.Get("XmlPath").ToString();
        private readonly IEventLogStore CsvGeneratorLog;

        public XmlProcess(IEventLogStore csvGeneratorLog)
        {
            CsvGeneratorLog = csvGeneratorLog;
        }
        /// <summary>
        /// Proceso principal del apliocativo en el cual se llaman las demas funciones y es llamado por el servicio windows o el proyecto de prueba (aplicacion de consola)
        /// </summary>
        /// <param name="data">Objeto del tipo Xmldocumento con la estructura completa de la data a procesar</param>
        /// <returns> Devuelve el Xml generado a forma de string con la estructura del objeto recibido y convertido en base 64 </returns>
        public string GetBase64Xml(Xmldocumento data)
        {
            try
            {
                MainXmlModel mainData = new MainXmlModel() { doc = data };
                XmlSerializerNamespaces ns = new XmlSerializerNamespaces();
                ns.Add("", "");
                string xmlText;
                //convierte el objeto MainXmlModel en un XmlSerializable
                using (var ms = new MemoryStream())
                {
                    using (var xw = XmlWriter.Create(ms)) 
                    {
                        var serializer = new XmlSerializer(mainData.GetType());
                        serializer.Serialize(xw, mainData, ns);
                        xw.Flush();
                        ms.Seek(0, SeekOrigin.Begin);
                        var sr = new StreamReader(ms, Encoding.UTF8);
                        xmlText = sr.ReadToEnd();
                    }
                }
                GenerateFile(xmlText, data.DOCNUM);
                //Codifica el xml string en UTF8
                var plainTextBytes = Encoding.UTF8.GetBytes(xmlText);
                //devuelve el string convirtiendolo en base 64
                return Convert.ToBase64String(plainTextBytes);
            }
            catch (Exception exp)
            {
                CsvGeneratorLog.StoreLog($"{this.ToString()}_GetBase64Xml  {exp.Message}", EventLogEntryType.Error);
                return "";
            }
        }

        private void GenerateFile(string xmlText, string docNum)
        {
            try
            {
                string pathXml = $"{XmlPath}\\{docNum}.xml";
                if (File.Exists(pathXml))
                    File.Delete(pathXml);
                File.AppendAllText(pathXml, xmlText);
            }
            catch (Exception exp)
            {
                CsvGeneratorLog.StoreLog($"{this.ToString()} GenerateFile {exp.Message}", EventLogEntryType.Error);
            }
        }
    }
}
