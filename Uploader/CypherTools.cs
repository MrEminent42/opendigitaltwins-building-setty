using System;
using System.Text;
using System.Collections.Generic;
using Neo4jClient.Cypher;

namespace Helper
{

    class PropsBuilder
    {
        private string identifier;
        private Dictionary<string, string> props;

        public PropsBuilder(string identifier)
        {
            this.identifier = identifier;
            this.props = new Dictionary<string, string>();
        }

        public void Add(string prop, string value)
        {
            this.props.Add(prop, value);
        }

        // @Deprecated
        private string ToCypher()
        {
            List<string> commands = new List<string>();

            foreach (KeyValuePair<string, string> propSet in props)
            {
                commands.Add($"{identifier}.{propSet.Key} = '{StringHelper.FormatApostrophes(propSet.Value)}'");
            }

            return String.Join(", ", commands);
        }

        public ICypherFluentQuery ApplyProps(ICypherFluentQuery command)
        {
            if (props.Count == 0) return command;
            return command.Set(ToCypher());
        }

    }

}