using Microsoft.Extensions.DependencyInjection;

using Microsoft.Extensions.Hosting;

using PnP.Core.Auth.Services;

using PnP.Core.Auth.Services.Builder.Configuration;

using PnP.Core.Services;

using PnP.Core.Services.Builder.Configuration;

using System;

using System.Collections.Generic;

using System.Configuration;

using System.Linq;

using System.Net;

using System.Security;

using System.Text;

using System.Threading.Tasks;

namespace NAA_WebBilling_CreateMaitenanceInvoice_SPO
{
    public class PnPSharePointContext
    {
        static string spoAccount = ConfigurationManager.AppSettings["spoaccount"];

        static string spoPassword = ConfigurationManager.AppSettings["spopassword"];

        static string sClientId = ConfigurationManager.AppSettings["ClientId"];

        //static string sClientSecret = ConfigurationManager.AppSettings["ClientSecret"];

        static string sTenantId = ConfigurationManager.AppSettings["TenantId"];
        public static IHost CreatePnPConfiguration()

        {
            System.Text.UTF8Encoding encoder = new System.Text.UTF8Encoding();

            System.Text.Decoder utf8Decode = encoder.GetDecoder();

            byte[] todecode_byte = Convert.FromBase64String(spoPassword);

            int charCount = utf8Decode.GetCharCount(todecode_byte, 0, todecode_byte.Length);

            char[] decoded_char = new char[charCount];

            utf8Decode.GetChars(todecode_byte, 0, todecode_byte.Length, decoded_char, 0);

            string result = new String(decoded_char);



            SecureString securepass = new NetworkCredential("", result).SecurePassword;



            var host = Host.CreateDefaultBuilder()

             // Configure logging 

             .ConfigureServices((hostingContext, services) =>

             {

                 // Add the PnP Core SDK library services 

                 services.AddPnPCore();

                 // Add the PnP Core SDK library services configuration from the appsettings.json file 

                 services.Configure<PnPCoreOptions>(hostingContext.Configuration.GetSection("PnPCore"));

                 // Add the PnP Core SDK Authentication Providers 

                 services.AddPnPCoreAuthentication();

                 // Add the PnP Core SDK Authentication Providers configuration from the appsettings.json file 

                 services.Configure<PnPCoreAuthenticationOptions>(hostingContext.Configuration.GetSection("PnPCore"));



                 services.AddPnPCoreAuthentication(options =>

                 {

                     options.Credentials.Configurations.Add("usernamepassword",

                       new PnPCoreAuthenticationCredentialConfigurationOptions

                       {

                           ClientId = sClientId,

                           TenantId = sTenantId,



                           //OnBehalfOf = new PnPCoreAuthenticationOnBehalfOfOptions 

                           //{ 



                           //    ClientSecret = sClientSecret 

                           //} 

                           //, 

                           UsernamePassword = new PnPCoreAuthenticationUsernamePasswordOptions

                           {





                               RedirectUri = new Uri("https://localhost"),

                               Username = spoAccount,

                               Password = result

                           }

                       });



                 });



             })

             // Let the builder know we're running in a console 

             .UseConsoleLifetime()

             // Add services to the container 

             .Build();

            // await host.StartAsync(); 

            return host;



        }



        /// <summary> 

        /// Get the PnPContext for provided Site URL 

        /// </summary> 

        /// <param name="siteURL">Enter the Site URL</param> 

        /// <returns>Return the PnPContext</returns> 

        public static async Task<PnPContext> GetPnPContext(String siteURL)

        {

            PnPContext context;

            var host = CreatePnPConfiguration();

            await host.StartAsync();

            //IHost host = (IHost)mytask; 

            using (var scope = host.Services.CreateScope())

            {

                var pnpContextFactory = scope.ServiceProvider.GetRequiredService<IPnPContextFactory>();

                var pnpAuthenticationProviderFactory = scope.ServiceProvider.GetRequiredService<IAuthenticationProviderFactory>();



                //var interactiveAuthProvider = pnpAuthenticationProviderFactory.Create("interactive"); 

                var passwordManagerAuthProvider = pnpAuthenticationProviderFactory.Create("usernamepassword");



                context = await pnpContextFactory.CreateAsync(new Uri(siteURL), passwordManagerAuthProvider);



            }

            return context;



        }
    }
}
