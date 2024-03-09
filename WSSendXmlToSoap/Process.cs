using Business;
using Log;
using System;
using System.Collections.Generic;
using System.ComponentModel;
using System.Configuration;
using System.Data;
using System.Diagnostics;
using System.Linq;
using System.ServiceProcess;
using System.Text;
using System.Threading.Tasks;

namespace WSSendXmlToSoap
{
    public partial class Process : ServiceBase
    {
        private bool ActiveProccess = false;
        private string ServiceName = ConfigurationSettings.AppSettings.Get("ServiceName").ToString();
        private string ProcessName = ConfigurationSettings.AppSettings.Get("ProcessName").ToString();
        private readonly IEventLogStore CsvGeneratorLog;
        private readonly IGeneralProcess generalProcess;
        public Process()
        {
            InitializeComponent();
        }

        public Process(IEventLogStore _CsvGeneratorLog, IGeneralProcess _generalProcess)
        {
            InitializeComponent();
            this.generalProcess = _generalProcess;
            this.CsvGeneratorLog = _CsvGeneratorLog;
            CsvGeneratorLog.StoreLog($"El servicio se ha inicio. ", EventLogEntryType.Information);
        }

        /// <summary>
		/// Se ejecuta cuando inica el proceso donde se configura el times, tiempo de ejecucion y funcion a ejecutar
		/// </summary>
		/// <returns> void  </returns>
        protected override void OnStart(string[] args)
        {
            CsvGeneratorLog.StoreLog($"El servicio {ServiceName} se ha inicio. ", EventLogEntryType.Information);
            System.Timers.Timer timer = new System.Timers.Timer();
            timer.Interval = 60000; // 60 seconds  
            timer.Elapsed += timer_Elapsed;
            timer.Start();
        }

        /// <summary>
		/// Se ejecuta cuando se detiene el proceso donde se podria configurar o detener el timer
		/// </summary>
		/// <returns> void  </returns>
        protected override void OnStop()
        {
            CsvGeneratorLog.StoreLog($"El servicio {ServiceName} se ha terminado. ", EventLogEntryType.Warning);
        }

        /// <summary>
		/// Funcion que se ejecutara cada X segundos dependiendo lo configurado anteriormente
		/// </summary>
		/// <returns> void  </returns>
        private void timer_Elapsed(object sender, System.Timers.ElapsedEventArgs e)
        {
            try
            {
                //Valida que el proceso no se este ejecutando.
                if (ActiveProccess)
                {
                    CsvGeneratorLog.StoreLog($"El proceso {ProcessName} se inicio pero esta ejecutando uno anterior. ", EventLogEntryType.Warning);
                    return;
                }

                ActiveProccess = true;
                CsvGeneratorLog.StoreLog($"El proceso {ProcessName} se inicio. ", EventLogEntryType.Information);
                //Llama el proceso principal para ejecutarlo
                generalProcess.MainProcess();

                CsvGeneratorLog.StoreLog($"El proceso {ProcessName} se Termino. ", EventLogEntryType.Information);
                ActiveProccess = false;
            }
            catch (Exception exp)
            {
                CsvGeneratorLog.StoreLog(this.ToString() + "  " + exp.Message, EventLogEntryType.Error);
                ActiveProccess = false;
            }
        }
    }
}
